package org.apache.kafka.streams.kstream.internals;

import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.retryableTestSupport.Pair;
import org.apache.kafka.retryableTestSupport.RetryableTestDrivers.RetryableProcessorTestDriver;
import org.apache.kafka.retryableTestSupport.RetryableTestDrivers.RetryableTestDriver;
import org.apache.kafka.retryableTestSupport.WithRetryableMockProcessorContext;
import org.apache.kafka.retryableTestSupport.extentions.TopologyPropertiesExtension;
import org.apache.kafka.retryableTestSupport.extentions.mockCallbacks.MockFailableExceptionForeachExtension;
import org.apache.kafka.retryableTestSupport.extentions.mockCallbacks.MockRetryableExceptionForeachExtension;
import org.apache.kafka.retryableTestSupport.extentions.mockCallbacks.MockSuccessfulForeachExtension;
import org.apache.kafka.retryableTestSupport.mocks.mockCallbacks.MockFailableExceptionForeach;
import org.apache.kafka.retryableTestSupport.mocks.mockCallbacks.MockRetryableExceptionForeach;
import org.apache.kafka.retryableTestSupport.mocks.mockCallbacks.MockSuccessfulForeach;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.TaskAttemptsStore.TaskAttemptsDAO;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.kafka.retryableTestSupport.TaskAttemptsStoreTestAccess.access;
import static org.apache.kafka.retryableTestSupport.TopologyFactory.createTopologyProps;
import static org.apache.kafka.retryableTestSupport.assertions.AttemptStoreAssertions.expect;
import static org.apache.kafka.retryableTestSupport.assertions.CallbackAssertions.expect;
import static org.apache.kafka.retryableTestSupport.assertions.LogAssertions.expect;
import static org.apache.kafka.retryableTestSupport.assertions.ScheduleKeyValueAssertions.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


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
        expect(processorTestDriver.getAction()).toHaveReceivedExactlyOneCall(new Pair<>("key", "value"));
    }

    @ParameterizedTest
    @DisplayName("On punctuate, it executes all retries scheduled to be attempted at or before the time of punctuation")
    @MethodSource("retryableProcessorDriverProvider")
    void testPerformRetry(RetryableProcessorTestDriver<String, String> processorTestDriver){
        ZonedDateTime now = ZonedDateTime.now();
        TaskAttemptsDAO dao = processorTestDriver.getTaskAttemptsDAO();
        expect(processorTestDriver.getAttemptStore()).toBeEmpty();

        dao.schedule(createTestTaskAttempt("veryOldKey", "veryOldValue", now.minusDays(5), processorTestDriver));
        dao.schedule(createTestTaskAttempt("recentOldKey1", "recentOldValue1", now.minusSeconds(1), processorTestDriver));
        dao.schedule(createTestTaskAttempt("recentOldKey2", "recentOldValue2", now.minus(500, ChronoUnit.MILLIS), processorTestDriver));
        dao.schedule(createTestTaskAttempt("scheduledInCurrentWindowKey1", "scheduleInCurrentWindowValue1", now.plus(500, ChronoUnit.MILLIS), processorTestDriver));
        dao.schedule(createTestTaskAttempt("scheduledInCurrentWindowKey2", "scheduleInCurrentWindowValue2", now.plusSeconds(1), processorTestDriver));
        dao.schedule(createTestTaskAttempt("afterWindowKey", "afterWindowValue", now.plusSeconds(2), processorTestDriver));


        // Advance stream time by 1 second
        processorTestDriver.advanceStreamTime(1000L);

        expect(processorTestDriver.getAction()).toHaveReceivedExactlyCalls(Arrays.asList(
                new Pair<>("veryOldKey", "veryOldValue"),
                new Pair<>("recentOldKey1", "recentOldValue1"),
                new Pair<>("recentOldKey2", "recentOldValue2"),
                new Pair<>("scheduledInCurrentWindowKey1", "scheduleInCurrentWindowValue1"),
                new Pair<>("scheduledInCurrentWindowKey1", "scheduleInCurrentWindowValue1")
                ));
    }

    @ParameterizedTest
    @DisplayName("It punctuates every half a second")
    @MethodSource("retryableProcessorDriverProvider")
    void testPunctuateSchedule(RetryableProcessorTestDriver<String, String> processorTestDriver){
        assertEquals(500L, processorTestDriver.getProcessorContext().scheduledPunctuators().get(0).getIntervalMs());
    }


    @ParameterizedTest
    @DisplayName("It deletes an attempt from the attempts store once that attempt has been executed")
    @MethodSource("retryableProcessorDriverProvider")
    void testRetryDeletionOnSuccess(RetryableProcessorTestDriver<String, String> processorTestDriver){
        TaskAttemptsDAO taskAttemptsDAO = processorTestDriver.getTaskAttemptsDAO();
        expect(processorTestDriver.getAttemptStore()).toBeEmpty();

        // Add an attempt to the store
        TaskAttempt testAttempt = createTestTaskAttempt("key", "value", ZonedDateTime.now(), processorTestDriver);
        taskAttemptsDAO.schedule(testAttempt);
        expect(processorTestDriver.getAttemptStore()).toHaveStoredTaskAttemptsCountOf(1);

        // Execute the attempt, then assert that the attempt is no longer in the store
        KeyValue<Long, TaskAttempt> existingAttempt = access(processorTestDriver.getAttemptStore()).getAllTaskAttempts().get(0);
        processorTestDriver.advanceStreamTimeTo(testAttempt.getTimeOfNextAttempt());

        expect(processorTestDriver.getAttemptStore()).toNotHaveAttempt(existingAttempt);
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
            expect(processorTestDriver.getAttemptStore()).toBeEmpty();
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
        @DisplayName("It schedules a retry via the attempts store if a RetryableException is thrown by the block on initial processing")
        void testSchedulingRetry(){
            processorTestDriver.pipeInput("key", "value");

            expect(processorTestDriver.getAttemptStore()).toHaveStoredTaskAttemptsCountOf(1);
            expect(access(processorTestDriver.getAttemptStore()).getAllTaskAttempts().get(0))
                    .toBeAttemptForMessage("key", "value",
                                            processorTestDriver.getInputTopicName(),
                                            processorTestDriver.getDefaultKeySerde().deserializer(),
                                            processorTestDriver.getDefaultValueSerde().deserializer());
        }


        @Test
        @DisplayName("It schedules another retry via the attempts store if an attempt is executed and throws a RetryableException")
        void testRetryRetryOnRetryableException(){
            TaskAttemptsDAO taskAttemptsDAO = processorTestDriver.getTaskAttemptsDAO();
            expect(processorTestDriver.getAttemptStore()).toBeEmpty();

            // Add an attempt to the store
            ZonedDateTime timeOfFirstAttempt = ZonedDateTime.now();
            TaskAttempt testAttempt = createTestTaskAttempt("key", "value", timeOfFirstAttempt, processorTestDriver);
            taskAttemptsDAO.schedule(testAttempt);
            Integer previousAttemptsCount = testAttempt.getAttemptsCount();

            // Execute retry
            processorTestDriver.advanceStreamTimeTo(timeOfFirstAttempt);

            // Assert a new attempt has been scheduled
            expect(processorTestDriver.getAttemptStore()).toHaveStoredTaskAttemptsCountOf(1);
            KeyValue<Long, TaskAttempt> scheduledAttempt = access(processorTestDriver.getAttemptStore()).getAllTaskAttempts().get(0);
            expect(scheduledAttempt).toBeScheduledLaterThan(timeOfFirstAttempt);
            expect(scheduledAttempt).toHaveMoreAttemptsThan(previousAttemptsCount);

            expect(access(processorTestDriver.getAttemptStore()).getAllTaskAttempts().get(0))
                    .toBeAttemptForMessage("key", "value",
                            processorTestDriver.getInputTopicName(),
                            processorTestDriver.getDefaultKeySerde().deserializer(),
                            processorTestDriver.getDefaultValueSerde().deserializer());
        }

        @Test
        @DisplayName("It treats a job that has exhausted it's retries as having thrown a FailableException")
        void testRetryExhaustionException() throws IOException {
            // Use the KeyValueStore directly instead of DAO in order to schedule a task attempt that has exhausted attempts
            expect(processorTestDriver.getAttemptStore()).toBeEmpty();

            // Create an attempt, advance it to it's final attempt, then add it to the store
            TaskAttempt testAttempt = createTestTaskAttempt("key", "value", ZonedDateTime.now(), processorTestDriver);
            while (!testAttempt.hasExhaustedRetries()){
                testAttempt.prepareForNextAttempt();
            }
            access(processorTestDriver.getAttemptStore()).addAttemptAt(testAttempt.getTimeOfNextAttempt(), testAttempt);

            // Execute retry
            processorTestDriver.advanceStreamTimeTo(testAttempt.getTimeOfNextAttempt());

            // No new attempt should be scheduled
            expect(processorTestDriver.getAttemptStore()).toBeEmpty();

            // The Dead Letter Topic should receive a message
            assertMessageForwardedToDLT(processorTestDriver, testAttempt.getTimeReceived().toInstant().toEpochMilli(), 11);
        }


        @Test
        @DisplayName("It logs an WARN that an retryable error has occurred")
        void testLogging(){
            processorTestDriver.pipeInput("key", "value");

            expect(logAppender).toHaveLoggedExactly(Level.WARN,
                    "A Retryable Error has occurred while processing the following message"
                    + "\n\tAttempt Number:\t1"
                    + "\n\tTopic:\t\t" + processorTestDriver.getInputTopicName()
                    + "\n\tKey:\t\tkey"
                    + "\n\tError:\t\t" + processorTestDriver.getAction().getException().toString()
            );
        }
    }

    /*
     * Test cases specific to behavior when the action throws a RetryableException
     */
    @Nested
    @DisplayName("When supplied with a lambda that throws a FailableException")
    @ExtendWith(MockFailableExceptionForeachExtension.class)
    class WhenFailableExceptions extends WithRetryableMockProcessorContext {
        WhenFailableExceptions(MockFailableExceptionForeach<String, String> action, Properties topologyProps){
            super(action, topologyProps);
        }

        @Test
        @DisplayName("It does not schedule a retry via the attempts store when a FailableException is thrown by the block")
        void testNoRetryScheduledOnFailableException(){
            expect(processorTestDriver.getAttemptStore()).toBeEmpty();
            processorTestDriver.pipeInput("key", "value");
            expect(processorTestDriver.getAttemptStore()).toBeEmpty();
        }


        @Test
        @DisplayName("It publishes a message to a dead letter topic when a FailableException is thrown by the block")
        void testPublishingFailableException() throws IOException {
            long now = ZonedDateTime.now().toInstant().toEpochMilli();
            processorTestDriver.pipeInput("key", "value");
            assertMessageForwardedToDLT(processorTestDriver, now, 1);
        }


        @Test
        @DisplayName("It logs a ERROR that an unretryable error has occurred")
        void testLogging(){
            processorTestDriver.pipeInput("key", "value");

            expect(logAppender).toHaveLoggedExactly(Level.ERROR,
                    "A Non-Retryable Error has occurred while processing the following message"
                    + "\n\tTopic:\t\t" + processorTestDriver.getInputTopicName()
                    + "\n\tKey:\t\tkey"
                    + "\n\tError:\t\t" + processorTestDriver.getAction().getException().toString()
            );
        }
    }


    /*
     * Assert that the processor sent a message to the Dead Letter Topic
     */
    private void assertMessageForwardedToDLT(RetryableProcessorTestDriver<String, String> processorTestDriver, Long timeOfReceipt, Integer attemptCount) throws IOException{
        List<MockProcessorContext.CapturedForward> forwarded = processorTestDriver.getForwardsToDeadLetterTopic();
        assertEquals(1, forwarded.size());
        String forwardedKey = (String) forwarded.get(0).keyValue().key;
        String forwardedAttemptData = (String) forwarded.get(0).keyValue().value;

        // Assert that the key is a combination of receiving topic and time the message was received
        assertTrue(forwardedKey.startsWith(processorTestDriver.getInputTopicName()));
        assertTrue(ZonedDateTime.parse(forwardedKey.split("\\.", 2)[1]).toInstant().toEpochMilli() >= timeOfReceipt);

        // Assert that the forwardedValue has useful data
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> attemptMap = objectMapper.readValue(forwardedAttemptData, new TypeReference<Map<String,Object>>(){});
        assertEquals(processorTestDriver.getInputTopicName(), attemptMap.get("topicOfOrigin"));
        assertTrue(ZonedDateTime.parse((String)attemptMap.get("timeReceived")).toInstant().toEpochMilli() >= timeOfReceipt);
        assertEquals(attemptCount, Integer.parseInt(((String)attemptMap.get("attempts"))));
        Map<String, Object> messageMap = objectMapper.readValue((String)attemptMap.get("message"), new TypeReference<Map<String,Object>>(){});
        assertEquals("key", messageMap.get("key"));
        assertEquals("value", messageMap.get("value"));
    }


    private TaskAttempt createTestTaskAttempt(String key, String value, ZonedDateTime timeOfNextAttempt, RetryableTestDriver<String, String> retryableTestDriver){
        String topicName = retryableTestDriver.getInputTopicName();
        TaskAttempt taskAttempt = new TaskAttempt(
                topicName,
                retryableTestDriver.getDefaultKeySerde().serializer().serialize(topicName, key),
                retryableTestDriver.getDefaultValueSerde().serializer().serialize(topicName, value)
        );
        taskAttempt.setTimeOfNextAttempt(timeOfNextAttempt);
        return taskAttempt;
    }

    private static Stream<RetryableProcessorTestDriver<String, String>> retryableProcessorDriverProvider(){
        final Serde<String> stringSerde = Serdes.String();

        return Stream.of(new MockSuccessfulForeach<String, String>(),
                         new MockRetryableExceptionForeach<String, String>(),
                         new MockFailableExceptionForeach<String, String>())
                .map(mockCallback -> new RetryableProcessorTestDriver<>(mockCallback, createTopologyProps(), stringSerde, stringSerde));
    }
}
