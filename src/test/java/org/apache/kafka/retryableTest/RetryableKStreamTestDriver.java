package org.apache.kafka.retryableTest;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.kstream.internals.RetryableForeachAction;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

import java.util.*;
import java.util.stream.Collectors;

public class RetryableKStreamTestDriver<K, V>{
    private static final String RETRIES_STORE_SUFFIX = "-RETRIES_STORE";
    private static final String RETRYABLE_FOREACH_PREFIX = "KSTREAM-RETRYABLE_FOREACH-";
    private static final String DEFAULT_TEST_INPUT_TOPIC_NAME = "TestTopic";
    private static final String DEFAULT_TEST_NODE_NAME = "TestNode";
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Topology topology;
    private final String inputTopicName;
    private final TopologyTestDriver driver;
    private final ConsumerRecordFactory<K, V> consumerRecordFactory;

    public RetryableKStreamTestDriver(RetryableForeachAction<K, V> action, Serde<K> keySerde, Serde<V> valueSerde, Properties streamsProps){
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.inputTopicName = DEFAULT_TEST_INPUT_TOPIC_NAME;
        final StreamsBuilder builder = new StreamsBuilder();
        final Consumed<K, V> consumed = Consumed.with(this.keySerde, this.valueSerde);
        final KStream<K, V> kStream = builder.stream(inputTopicName, consumed);
        final RetryableKStream<K, V> retriableStream = RetryableKStream.fromKStream(kStream);
        retriableStream.retryableForeach(action, DEFAULT_TEST_NODE_NAME);

        this.topology = builder.build();
        this.driver = new TopologyTestDriver(this.topology, streamsProps);
        this.consumerRecordFactory = new ConsumerRecordFactory<>(inputTopicName,
                this.keySerde.serializer(), this.valueSerde.serializer());
    }

    /**
     * Enter a message into the test topology
     * @param key Key of the Kafka message
     * @param value Value of the Kafka message
     */
    public void pipeInput(K key, V value){
        driver.pipeInput(consumerRecordFactory.create(key, value));
    }

    /**
     * @return Topology Node of the default Retryable node in the test topology
     */
    public TopologyDescription.Node getRetryNode(){
        return getRetryNode(DEFAULT_TEST_NODE_NAME);
    }

    /**
     * @param name Retryable node to get
     * @return Topology Node of the specified Retryable node in the test topology
     */
    public TopologyDescription.Node getRetryNode(String name){
        return getAllRetryNodes().get(RETRYABLE_FOREACH_PREFIX.concat(name));
    }

    public Map<String, TopologyDescription.Node> getAllRetryNodes(){
        return getAllTopologyNodes()
                .stream()
                .filter(node -> node.name().startsWith(RETRYABLE_FOREACH_PREFIX))
                .collect(Collectors.toMap(node -> node.name(), node -> node));
    }

    /**
     * Get the state store used to store task attempt information in the test topology for the default Retryable node
     * @return KeyValueStateStore containing task attempt data
     */
    public KeyValueStore<Long, TaskAttempt> getAttemptStateStore(){
        return getAttemptStateStore(DEFAULT_TEST_NODE_NAME);
    }

    /**
     * Get the state store used to store task attempt information in the test topology for the specified node
     * @return KeyValueStateStore containing task attempt data
     */
    public KeyValueStore<Long, TaskAttempt> getAttemptStateStore(String nodeName){
        return driver.getKeyValueStore(RETRYABLE_FOREACH_PREFIX.concat(nodeName.concat(RETRIES_STORE_SUFFIX)));
    }

    /**
     * @return A List of all state stores in the test topology used to store retry information
     */
    public Map<String, StateStore> getAllAttemptStateStores(){
        return driver.getAllStateStores().entrySet()
                .stream()
                .filter(map -> map.getKey().endsWith("-RETRIES_STORE"))
                .collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()));
    }

    public Serde<K> getKeySerde() {
        return keySerde;
    }

    public Serde<V> getValueSerde() {
        return valueSerde;
    }

    public String getInputTopicName(){
        return inputTopicName;
    }

    public TopologyTestDriver getTopologyTestDriver(){
        return this.driver;
    }

    private Set<TopologyDescription.Node> getAllTopologyNodes(){
        return topology.describe().subtopologies()
                .stream()
                .collect(
                        HashSet::new,
                        (allNodes, subTopology) -> allNodes.addAll(subTopology.nodes()),
                        (AbstractCollection::addAll)
                );
    }


}
