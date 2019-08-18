package org.apache.kafka.retryableTest;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.kstream.internals.RetryableForeachAction;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

import java.util.Properties;

public class RetryableKStreamTestDriver<K, V>{
    private static final String TEST_INPUT_TOPIC_NAME = "TestTopic";
    private static final String TEST_NODE_NAME = "TestNode";
    private final TopologyTestDriver driver;
    private final ConsumerRecordFactory<K, V> consumerRecordFactory;

    public RetryableKStreamTestDriver(RetryableForeachAction<K, V> action, Serde<K> keySerde, Serde<V> valueSerde, Properties streamsProps){
        final StreamsBuilder builder = new StreamsBuilder();
        final Consumed<K, V> consumed = Consumed.with(keySerde, valueSerde);
        final KStream<K, V> kStream = builder.stream(TEST_INPUT_TOPIC_NAME, consumed);
        final RetryableKStream<K, V> retriableStream = RetryableKStream.fromKStream(kStream);
        retriableStream.retryableForeach(action, TEST_NODE_NAME);

        this.driver = new TopologyTestDriver(builder.build(), streamsProps);
        this.consumerRecordFactory = new ConsumerRecordFactory<>(TEST_INPUT_TOPIC_NAME,
                keySerde.serializer(), valueSerde.serializer());
    }

    /**
     * Enter a message into the test topology
     * @param key
     * @param value
     */
    public void pipeInput(K key, V value){
        driver.pipeInput(consumerRecordFactory.create(key, value));
    }

    /**
     * Get the state store used to store job retry information in the test topology
     * @return KeyValueStateStore containing retry data
     */
    public KeyValueStore<String, String> getRetriesStateStore(){
        return driver.getKeyValueStore(TEST_NODE_NAME + "-RETRIES_STORE");
    }

    public TopologyTestDriver getTopologyTestDriver(){
        return this.driver;
    }

}
