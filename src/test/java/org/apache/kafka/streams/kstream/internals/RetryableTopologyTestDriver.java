package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.retryableTest.TestTopology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.retryableTest.TestTopology.DEFAULT_TEST_NODE_NAME;

public class RetryableTopologyTestDriver<K, V> implements RetryableTestDriver<K, V> {
    private static final String RETRIES_STORE_SUFFIX = "-RETRIES_STORE";
    private final TestTopology<K, V> testTopology;
    private final TopologyTestDriver driver;
    private final ConsumerRecordFactory<K, V> consumerRecordFactory;

    public RetryableTopologyTestDriver(TestTopology<K, V> testTopology, Properties streamsProps){
        this.testTopology = testTopology;

        this.driver = new TopologyTestDriver(this.testTopology.getTopology(), streamsProps);
        this.consumerRecordFactory = new ConsumerRecordFactory<>(testTopology.getInputTopicName(),
                this.testTopology.getInputKeySerde().serializer(),
                this.testTopology.getInputValueSerde().serializer());
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
     * Get the state store used to store task attempt information in the test topology for the default Retryable node
     * @return KeyValueStateStore containing task attempt data
     */
    @Override
    public KeyValueStore<Long, TaskAttemptsCollection> getAttemptStore(){
        return getAttemptStore(DEFAULT_TEST_NODE_NAME);
    }

    /**
     * Get the state store used to store task attempt information in the test topology for the specified node
     * @return KeyValueStateStore containing task attempt data
     */
    public KeyValueStore<Long, TaskAttemptsCollection> getAttemptStore(String nodeName){
        return driver.getKeyValueStore(TestTopology.RETRYABLE_FOREACH_PREFIX.concat(nodeName.concat(RETRIES_STORE_SUFFIX)));
    }

    /**
     * @return A TaskAttemptsDAO associated with the state store used for the default Retryable node
     */
    @Override
    public TaskAttemptsDAO getTaskAttemptsDAO(){
        return new TaskAttemptsDAO(getAttemptStore());
    }

    /**
     * @return A List of all state stores in the test topology used to store retry information
     */
    public Map<String, StateStore> getAllAttemptStores(){
        return driver.getAllStateStores().entrySet()
                .stream()
                .filter(map -> map.getKey().endsWith("-RETRIES_STORE"))
                .collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()));
    }

    public TestTopology getTestTopology() {
        return testTopology;
    }

    public TopologyTestDriver getTopologyTestDriver(){
        return this.driver;
    }

    @Override
    public Serde<K> getDefaultKeySerde() {
        return this.testTopology.getInputKeySerde();
    }

    @Override
    public Serde<V> getDefaultValueSerde() {
        return this.testTopology.getInputValueSerde();
    }

    @Override
    public String getInputTopicName(){
        return testTopology.getInputTopicName();
    }

}
