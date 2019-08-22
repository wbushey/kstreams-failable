package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.retryableTest.RetryableTopologyTestDriver;
import org.apache.kafka.retryableTest.WithRetryableTopologyTestDriver;
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

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;


class KStreamRetryableForeachTest {

    @Nested
    @DisplayName("When supplied with a successful lambda")
    @ExtendWith(MockSuccessfulForeachExtension.class)
    class WhenSuccessfulAction extends WithRetryableTopologyTestDriver {
        WhenSuccessfulAction(MockSuccessfulForeach<String, String> action, Properties topologyProps){
            super(action, topologyProps);
        }

        @Test
        @DisplayName("It immediately attempts to execute the provided block")
        void testImmediateExecution(){
            retryableDriver.pipeInput("key", "value");
            assertEquals(Collections.singletonList(new Pair<>("key", "value")), action.getReceivedParameters());
        }


        @Test
        @DisplayName("It deletes a retry from the attempts state store once it is been executed successfully")
        void testRetryDeletionOnSuccess(){
            // Assert no attempts are currently in the store
            KeyValueStore<Long, TaskAttempt> attemptsStore = retryableDriver.getAttemptStateStore();
            assertEquals(0, attemptsStore.approximateNumEntries());

            // Add an attempt to the store
            TaskAttempt testAttempt = createTestTaskAttempt("key", "value", retryableDriver);
            retryableDriver.getAttemptStateStore().put(ZonedDateTime.now().toInstant().toEpochMilli(), testAttempt);
            assertEquals(1, attemptsStore.approximateNumEntries());

            // Execute the attempt, then assert that no attempts are in the store
            retryableDriver.getTopologyTestDriver().advanceWallClockTime(3000L);
            assertEquals(0, attemptsStore.approximateNumEntries());
        }


    }

    @Nested
    @DisplayName("When supplied with a lambda that throws a RetryableException")
    @ExtendWith(MockRetryableExceptionForeachExtension.class)
    class WhenRetryableExceptions extends WithRetryableTopologyTestDriver {
        WhenRetryableExceptions(MockRetryableExceptionForeach<String, String> action, Properties topologyProps){
            super(action, topologyProps);
        }

        @Test
        @DisplayName("It immediately attempts to execute the provided block")
        void testImmediateExecution(){
            retryableDriver.pipeInput("key", "value");
            assertEquals(Collections.singletonList(new Pair<>("key", "value")), action.getReceivedParameters());
        }


        @Test
        @DisplayName("It schedules a retry via the retries state store if a RetryableException is thrown by the block")
        void testSchedulingRetry(){
            retryableDriver.pipeInput("key", "value");

            List<KeyValue<Long, TaskAttempt>> scheduledJobs = new LinkedList<>();
            retryableDriver.getAttemptStateStore().all().forEachRemaining(scheduledJobs::add);

            assertEquals(1, scheduledJobs.size());
            Deserializer<String> keyDerializer = retryableDriver.getDefaultKeySerde().deserializer();
            Deserializer<String> valueDerializer = retryableDriver.getDefaultValueSerde().deserializer();

            assertEquals("key", keyDerializer.deserialize(retryableDriver.getInputTopicName(),
                                                                    scheduledJobs.get(0).value.getMessage().keyBytes));
            assertEquals("value", valueDerializer.deserialize(retryableDriver.getInputTopicName(),
                                                                        scheduledJobs.get(0).value.getMessage().valueBytes));
        }

        @Test
        @DisplayName("It executes a retry after some time has passed")
        void testPerformRetry(){
            retryableDriver.pipeInput("key", "value");
            assertEquals(1, action.getReceivedParameters().size());

            // Advance stream time by 3 second
            retryableDriver.getTopologyTestDriver().advanceWallClockTime(3000L);

            assertEquals(2, action.getReceivedParameters().size());
        }


    }



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


    private TaskAttempt createTestTaskAttempt(String key, String value, RetryableTopologyTestDriver<String, String> retryableTopologyTestDriver){
        String topicName = retryableTopologyTestDriver.getInputTopicName();
        return new TaskAttempt(
                topicName,
                retryableTopologyTestDriver.getDefaultKeySerde().serializer().serialize(topicName, key),
                retryableTopologyTestDriver.getDefaultValueSerde().serializer().serialize(topicName, value)
        );
    }
}

