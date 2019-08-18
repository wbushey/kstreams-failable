package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.retryableTest.extentions.topologyTestDriver.TopologyTestDriverExtension;
import org.apache.kafka.retryableTest.extentions.topologyTestDriver.TopologyTestDriverProvider;
import org.apache.kafka.retryableTest.mockCallbacks.MockForeach;
import org.apache.kafka.retryableTest.mockCallbacks.MockRetryableExceptionForeach;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.retryableTest.Pair;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith(org.apache.kafka.retryableTest.extentions.mockCallbacks.MockForeach.class)
class KStreamRetryableForeachTest implements TopologyTestDriverProvider  {
    /*
     * Mock ForeachActions and related helpers
     */
    private final MockForeach<String, String> mockForeach;


    /*
     * TopologyTestDriver supporting
     */
    private final String TEST_INPUT_TOPIC_NAME = "testTopic";
    private final Consumed<String, String> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());

    // Test constructor
    KStreamRetryableForeachTest(MockForeach<String, String> mockForeach){
        this.mockForeach = mockForeach;
    }

    @Test
    @DisplayName("It immediately attempts to execute the provided block")
    void testImmediateExecution(){
        final Processor<String, String> subject = new KStreamRetryableForeach<>("Test-Store", mockForeach.getCallback()).get();
        subject.process("key", "value");
        assertEquals(Collections.singletonList(new Pair<>("key", "value")), mockForeach.getReceivedRecords());
    }

    @Nested
    @ExtendWith(org.apache.kafka.retryableTest.extentions.mockCallbacks.MockRetryableExceptionForeach.class)
    @ExtendWith(TopologyTestDriverExtension.class)
    class WithRetryableExceptionsInTopologyTestDriver implements TopologyTestDriverProvider {
        private final TopologyTestDriver driver;
        private final ConsumerRecordFactory<String, String> consumerRecordFactory;

        WithRetryableExceptionsInTopologyTestDriver(MockRetryableExceptionForeach<String, String> mockRetryableExceptionForeach,
                                                    Properties topologyProps){
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> kStream = builder.stream(TEST_INPUT_TOPIC_NAME, stringConsumed);
            final RetryableKStream<String, String> retriableStream = RetryableKStream.fromKStream(kStream);
            final Serializer<String> stringSerializer = new StringSerializer();
            retriableStream.retryableForeach(mockRetryableExceptionForeach.getCallback(), "Test");

            this.driver = new TopologyTestDriver(builder.build(), topologyProps);
            this.consumerRecordFactory = new ConsumerRecordFactory<>(TEST_INPUT_TOPIC_NAME, stringSerializer, stringSerializer);
        }

        @Test
        @DisplayName("It schedules a retry via the retries state store if a RetryableException is thrown by the block")
        void testSchedulingRetry(){
            driver.pipeInput(consumerRecordFactory.create(TEST_INPUT_TOPIC_NAME, "key", "value"));
            List<KeyValue<String, String>> scheduledJobs = new LinkedList<>();
            final KeyValueStore<String, String> retriesStore = driver.getKeyValueStore("Test-RETRIES_STORE");
            retriesStore.all().forEachRemaining(scheduledJobs::add);
            assertEquals(1, scheduledJobs.size());
            assertEquals("value", scheduledJobs.get(0).value);
        }


        @Override
        public TopologyTestDriver getTopologyTestDriver() {
            return this.driver;
        }
    }

    @Disabled
    @Test
    @DisplayName("It deletes a retry from the retries state store once it is been executed successfully")
    void testRetryDeletionOnSuccess(){}

    @Disabled
    @Test
    @DisplayName("It deletes a retry from the retries state store once it is been executed and thrown a RetryableException")
    void testRetryDeletionOnRetryableException(){}

    @Disabled
    @Test
    @DisplayName("It deletes a retry from the retries state store once it is been executed and thrown a FailableException")
    void testRetryDeletionOnFailableException(){}

    @Disabled
    @Test
    @DisplayName("It schedules retries via an exponential backoff based on number of retries already attempted")
    void testExponentialBackoffScheduling(){}

    @Disabled
    @Test
    @DisplayName("It schedules another retry via the retries state store if a retry is executed and throws a RetryableException")
    void testRetryRetryOnRetryableException(){}

    @Disabled
    @Test
    @DisplayName("It schedules retry punctuation at appropriate interval")
    void testPunctuateScheduling(){}

    @Disabled
    @Test
    @DisplayName("It executes scheduled retries on punctuate")
    void testPunctuateExecution(){}

    @Disabled
    @Test
    @DisplayName("On punctuate, it executes scheduled retries recovered from application crash")
    void testRecoveringScheduledRetries(){}

    @Disabled
    @Test
    @DisplayName("On punctuate, it immediately executes scheduled retries recovered from application crash that should have already been executed")
    void testImmediateExecutionOfRecoveredScheduledRetries(){}

    @Disabled
    @Test
    @DisplayName("It publishes a message to a dead letter topic when a FailableException is thrown by the block")
    void testPublishingFailableException(){}

    @Disabled
    @Test
    @DisplayName("It does not schedule a retry via the retires state store when a FailableException is thrown by the block")
    void testNoRetryScheduledOnFailableException(){}

    @Disabled
    @Test
    @DisplayName("It does not schedule a retry via the retires state store when no exception is thrown by the block")
    void testNoRetryScheduledOnSuccess(){}

    @Disabled
    @Test
    @DisplayName("It treats a job that has exhausted it's retries as having thrown a FailableException")
    void testRetryExhaustionException(){}

    @Disabled
    @Test
    @DisplayName("It closes the scheduled retries state store when closed")
    void testCloseStateStoreOnClose(){}

    @Override
    public TopologyTestDriver getTopologyTestDriver() {
        return null;
    }
}

