package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.retryableTestSupport.mocks.mockCallbacks.MockCallback;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.TaskAttemptsStore.TaskAttemptsDAO;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class RetryableProcessorTestDriver<K, V> implements RetryableTestDriver<K, V> {
    private static final String DEFAULT_INPUT_TOPIC_NAME = "testTopic";
    private static final String DEAFULT_TEST_ATTEMPTS_STORE_NAME = "testAttemptsStore";
    private static final String DEFAULT_DEADLETTER_NODE_NAME = "testAttemptsStore";
    private final String inputTopicName;
    private final String deadLetterNodeName;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final MockCallback<K, V> action;
    private final MockProcessorContext mockContext;
    private final Processor<K, V> processor;
    private final KeyValueStore<Long, TaskAttemptsCollection> attemptsStore;
    private final TaskAttemptsDAO dao;

    public RetryableProcessorTestDriver(MockCallback<K, V> mockCallback, Properties topologyProps, Serde<K> keySerde, Serde<V> valueSerde) {
        this.inputTopicName = DEFAULT_INPUT_TOPIC_NAME;
        this.deadLetterNodeName = DEFAULT_DEADLETTER_NODE_NAME;
        this.action = mockCallback;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.processor = new KStreamRetryableForeach<>(DEAFULT_TEST_ATTEMPTS_STORE_NAME, deadLetterNodeName, action.getCallback()).get();

        this.mockContext = new MockProcessorContext(topologyProps);
        this.attemptsStore = StoreBuilders.getTaskAttemptsStoreBuilder(DEAFULT_TEST_ATTEMPTS_STORE_NAME, StoreBuilders.BackingStore.IN_MEMORY)
                .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                .build();
        this.dao = new TaskAttemptsDAO(attemptsStore);

        attemptsStore.init(mockContext, attemptsStore);
        mockContext.setTopic(inputTopicName);
        mockContext.register(attemptsStore, null);
        processor.init(mockContext);
    }

    public MockCallback<K, V> getAction() {
        return action;
    }

    public Processor<K, V> getProcessor() {
        return processor;
    }

    public MockProcessorContext getProcessorContext() {
        return mockContext;
    }

    public Punctuator getRetryPunctuator() {
        return mockContext.scheduledPunctuators().get(0).getPunctuator();
    }

    @Override
    public String getInputTopicName() {
        return inputTopicName;
    }

    @Override
    public Serde<K> getDefaultKeySerde() {
        return keySerde;
    }

    @Override
    public Serde<V> getDefaultValueSerde() {
        return valueSerde;
    }

    @Override
    public KeyValueStore<Long, TaskAttemptsCollection> getAttemptStore() {
        return attemptsStore;
    }

    @Override
    public TaskAttemptsDAO getTaskAttemptsDAO(){
        return dao;
    }

    @Override
    public void pipeInput(K key, V value) {
        processor.process(key, value);
    }

    public void advanceStreamTime(Long millis){
        getRetryPunctuator().punctuate(ZonedDateTime.now().toInstant().toEpochMilli() + millis);
    }

    public void advanceStreamTimeTo(ZonedDateTime time){
        advanceStreamTimeTo(time.toInstant().toEpochMilli());
    }

    public void advanceStreamTimeTo(Long time){
        getRetryPunctuator().punctuate(time);
    }


    public String getDeadLetterNodeName(){
        return deadLetterNodeName;
    }

    public List<MockProcessorContext.CapturedForward> getForwardsToDeadLetterTopic(){
        return getProcessorContext().forwarded(getDeadLetterNodeName());
    }
}
