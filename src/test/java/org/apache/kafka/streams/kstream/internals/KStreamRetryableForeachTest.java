package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.retryableTest.WithRetryableMockProcessorContext;
import org.apache.kafka.retryableTest.extentions.TopologyPropertiesExtension;
import org.apache.kafka.retryableTest.extentions.mockCallbacks.MockRetryableExceptionForeachExtension;
import org.apache.kafka.retryableTest.extentions.mockCallbacks.MockSuccessfulForeachExtension;
import org.apache.kafka.retryableTest.mockCallbacks.MockSuccessfulForeach;
import org.apache.kafka.retryableTest.mockCallbacks.MockRetryableExceptionForeach;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.retryableTest.Pair;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.kafka.retryableTest.TopologyFactory.createTopologyProps;
import static org.junit.jupiter.api.Assertions.*;


@ExtendWith(TopologyPropertiesExtension.class)
class KStreamRetryableForeachTest {


    /*
     * Test cases that apply regardless of whether the supplied action succeeds or throws an Exception
     */
    @ParameterizedTest
    @DisplayName("It immediately attempts to execute the provided block")
    @MethodSource("retryableProcessorDriverProvider")
    void testImmediateExecution(RetryableProcessorTestDriver<String, String> processorTestDriver){
        processorTestDriver.getProcessor().process("key", "value");
        assertEquals(Collections.singletonList(new Pair<>("key", "value")), processorTestDriver.getAction().getReceivedParameters());
    }

    @ParameterizedTest
    @DisplayName("On punctuate, it executes retries scheduled to be attempted at or before the time of punctuation")
    @MethodSource("retryableProcessorDriverProvider")
    void testPerformRetry(RetryableProcessorTestDriver<String, String> processorTestDriver){
        long now = ZonedDateTime.now().toInstant().toEpochMilli();
        KeyValueStore<Long, TaskAttempt> attemptsStore = processorTestDriver.getAttemptStore();
        assertEquals(0, attemptsStore.approximateNumEntries());

        attemptsStore.put(now + 500L, createTestTaskAttempt("inWindowKey1", "inWindowValue1", processorTestDriver));
        attemptsStore.put(now + 1000L, createTestTaskAttempt("inWindowKey2", "inWindowValue2", processorTestDriver));
        attemptsStore.put(now + 2000L, createTestTaskAttempt("afterWindowKey", "afterWindowValue", processorTestDriver));

        // Advance stream time by 1 second
        processorTestDriver.getRetryPunctuator().punctuate(now + 1000L);

        assertEquals(2, processorTestDriver.getAction().getReceivedParameters().size());
        assertTrue(processorTestDriver.getAction().getReceivedParameters().contains(new Pair<>("inWindowKey1", "inWindowValue1")));
        assertTrue(processorTestDriver.getAction().getReceivedParameters().contains(new Pair<>("inWindowKey2", "inWindowValue2")));
    }


    @ParameterizedTest
    @DisplayName("It punctuates every half a second")
    @MethodSource("retryableProcessorDriverProvider")
    void testPunctuateSchedule(RetryableProcessorTestDriver<String, String> processorTestDriver){
        assertEquals(500L, processorTestDriver.getContext().scheduledPunctuators().get(0).getIntervalMs());

    }


    @ParameterizedTest
    @DisplayName("It deletes an attempt from the attempts store once that attempt has been executed")
    @MethodSource("retryableProcessorDriverProvider")
    void testRetryDeletionOnSuccess(RetryableProcessorTestDriver<String, String> processorTestDriver){
        long now = ZonedDateTime.now().toInstant().toEpochMilli();
        KeyValueStore<Long, TaskAttempt> attemptsStore = processorTestDriver.getAttemptStore();
        assertEquals(0, attemptsStore.approximateNumEntries());

        // Add an attempt to the store
        TaskAttempt testAttempt = createTestTaskAttempt("key", "value", processorTestDriver);
        attemptsStore.put(now, testAttempt);
        assertEquals(1, attemptsStore.approximateNumEntries());


        // Execute the attempt, then assert that the attempt is no longer in the store
        KeyValue<Long, TaskAttempt> existingAttempt = processorTestDriver.getScheduledTaskAttempts().get(0);
        processorTestDriver.getRetryPunctuator().punctuate(now);
        List<KeyValue<Long, TaskAttempt>> scheduledTaskAttempts = processorTestDriver.getScheduledTaskAttempts();
        assertFalse(scheduledTaskAttempts.contains(existingAttempt));
    }


    /*
     * Test cases specific to behavior when the action succeed upon execution/retry
     */
    @Nested
    @DisplayName("When supplied with a successful lambda")
    @ExtendWith(MockSuccessfulForeachExtension.class)
    class WhenSuccessfulAction extends WithRetryableMockProcessorContext {
        WhenSuccessfulAction(MockSuccessfulForeach<String, String> action, Properties topologyProps){
            super(action, topologyProps);
        }

        @Test
        @DisplayName("It does not schedule a retry via the attempts store")
        void testSchedulingRetry(){
            processorTestDriver.pipeInput("key", "value");

            List<KeyValue<Long, TaskAttempt>> scheduledTaskAttempts = processorTestDriver.getScheduledTaskAttempts();

            assertEquals(0, scheduledTaskAttempts.size());
        }
    }


    /*
     * Test cases specific to behavior when the action throws a RetryableException
     */
    @Nested
    @DisplayName("When supplied with a lambda that throws a RetryableException")
    @ExtendWith(MockRetryableExceptionForeachExtension.class)
    class WhenRetryableExceptions extends WithRetryableMockProcessorContext{
        WhenRetryableExceptions(MockRetryableExceptionForeach<String, String> action, Properties topologyProps){
            super(action, topologyProps);
        }

        @Test
        @DisplayName("It schedules a retry via the retries state store if a RetryableException is thrown by the block")
        void testSchedulingRetry(){
            processorTestDriver.pipeInput("key", "value");

            List<KeyValue<Long, TaskAttempt>> scheduledTaskAttempts = processorTestDriver.getScheduledTaskAttempts();

            assertEquals(1, scheduledTaskAttempts.size());
            Deserializer<String> keyDerializer = processorTestDriver.getDefaultKeySerde().deserializer();
            Deserializer<String> valueDerializer = processorTestDriver.getDefaultValueSerde().deserializer();

            assertEquals("key", keyDerializer.deserialize(processorTestDriver.getInputTopicName(),
                                                                    scheduledTaskAttempts.get(0).value.getMessage().keyBytes));
            assertEquals("value", valueDerializer.deserialize(processorTestDriver.getInputTopicName(),
                                                                        scheduledTaskAttempts.get(0).value.getMessage().valueBytes));
        }



    }



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
    @DisplayName("It treats a job that has exhausted it's retries as having thrown a FailableException")
    void testRetryExhaustionException(){}

    @Disabled
    @Test
    @DisplayName("It closes the scheduled retries state store when closed")
    void testCloseStateStoreOnClose(){}


    private TaskAttempt createTestTaskAttempt(String key, String value, RetryableTestDriver<String, String> retryableTestDriver){
        String topicName = retryableTestDriver.getInputTopicName();
        return new TaskAttempt(
                topicName,
                retryableTestDriver.getDefaultKeySerde().serializer().serialize(topicName, key),
                retryableTestDriver.getDefaultValueSerde().serializer().serialize(topicName, value)
        );
    }

    private static Stream<RetryableProcessorTestDriver<String, String>> retryableProcessorDriverProvider(){
        final Serde<String> stringSerde = Serdes.String();

        return Stream.of(new MockSuccessfulForeach<String, String>(),
                         new MockRetryableExceptionForeach<String, String>())
                .map(mockCallback -> new RetryableProcessorTestDriver<>(mockCallback, createTopologyProps(), stringSerde, stringSerde));
    }
}

