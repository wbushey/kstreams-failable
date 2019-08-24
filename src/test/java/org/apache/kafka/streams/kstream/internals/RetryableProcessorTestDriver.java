package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.retryableTest.mockCallbacks.MockCallback;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.internals.serdes.TaskAttemptSerde;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class RetryableProcessorTestDriver<K, V> implements RetryableTestDriver<K, V> {
    private static final String DEFAULT_INPUT_TOPIC_NAME = "testTopic";
    private static final String TEST_ATTEMPTS_STORE_NAME = "testAttemptsStore";
    private final String inputTopicName;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final MockCallback<K, V> action;
    private final MockProcessorContext mockContext;
    private final Processor<K, V> processor;
    private final KeyValueStore<Long, TaskAttempt> attemptsStore;

    public RetryableProcessorTestDriver(MockCallback<K, V> mockCallback, Properties topologyProps, Serde<K> keySerde, Serde<V> valueSerde){
        this.inputTopicName = DEFAULT_INPUT_TOPIC_NAME;
        this.action = mockCallback;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.processor = new KStreamRetryableForeach<>(TEST_ATTEMPTS_STORE_NAME, action.getCallback()).get();

        this.mockContext = new MockProcessorContext(topologyProps);
        this.attemptsStore =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(TEST_ATTEMPTS_STORE_NAME),
                        Serdes.Long(),
                        new TaskAttemptSerde()
                )
                        .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                        .build();
        attemptsStore.init(mockContext, attemptsStore);
        mockContext.setTopic(inputTopicName);
        mockContext.register(attemptsStore, null);
        processor.init(mockContext);
    }

    public MockCallback<K, V> getAction(){
        return action;
    }

    public Processor<K, V> getProcessor(){
        return processor;
    }

    public MockProcessorContext getContext(){
        return mockContext;
    }

    public Punctuator getRetryPunctuator(){
        return mockContext.scheduledPunctuators().get(0).getPunctuator();
    }

    @Override
    public String getInputTopicName(){
        return inputTopicName;
    }

    @Override
    public Serde<K> getDefaultKeySerde(){
       return keySerde;
    }

    @Override
    public Serde<V> getDefaultValueSerde(){
        return valueSerde;
    }

    @Override
    public KeyValueStore<Long, TaskAttempt> getAttemptStore() {
        return attemptsStore;
    }

    @Override
    public void pipeInput(K key, V value) {
        processor.process(key, value);
    }

    public List<KeyValue<Long, TaskAttempt>> getScheduledTaskAttempts(){
        List<KeyValue<Long, TaskAttempt>> scheduledTaskAttempts = new LinkedList<>();
        getAttemptStore().all().forEachRemaining(scheduledTaskAttempts::add);
        return scheduledTaskAttempts;
    }

}
