package org.apache.kafka.streams.kstream.internals;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.retryableTest.WithRetryableMockProcessorContext;
import org.apache.kafka.retryableTest.extentions.TopologyPropertiesExtension;
import org.apache.kafka.retryableTest.extentions.mockCallbacks.MockFailableExceptionForeachExtension;
import org.apache.kafka.retryableTest.extentions.mockCallbacks.MockRetryableExceptionForeachExtension;
import org.apache.kafka.retryableTest.extentions.mockCallbacks.MockSuccessfulForeachExtension;
import org.apache.kafka.retryableTest.mocks.mockCallbacks.MockFailableExceptionForeach;
import org.apache.kafka.retryableTest.mocks.mockCallbacks.MockSuccessfulForeach;
import org.apache.kafka.retryableTest.mocks.mockCallbacks.MockRetryableExceptionForeach;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.retryableTest.Pair;
import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
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
    @DisplayName("On punctuate, it executes all retries scheduled to be attempted at or before the time of punctuation")
    @MethodSource("retryableProcessorDriverProvider")
    void testPerformRetry(RetryableProcessorTestDriver<String, String> processorTestDriver){
        ZonedDateTime now = ZonedDateTime.now();
        KeyValueStore<Long, TaskAttemptsCollection> attemptsStore = processorTestDriver.getAttemptStore();
        TaskAttemptsDAO dao = processorTestDriver.getTaskAttemptsDAO();
        assertEquals(0, processorTestDriver.getCountOfScheduledTaskAttempts());

        dao.schedule(createTestTaskAttempt("veryOldKey", "veryOldValue", now.minusDays(5), processorTestDriver));
        dao.schedule(createTestTaskAttempt("recentOldKey1", "recentOldValue1", now.minusSeconds(1), processorTestDriver));
        dao.schedule(createTestTaskAttempt("recentOldKey2", "recentOldValue2", now.minus(500, ChronoUnit.MILLIS), processorTestDriver));
        dao.schedule(createTestTaskAttempt("scheduledInCurrentWindowKey1", "scheduleInCurrentWindowValue1", now.plus(500, ChronoUnit.MILLIS), processorTestDriver));
        dao.schedule(createTestTaskAttempt("scheduledInCurrentWindowKey2", "scheduleInCurrentWindowValue2", now.plusSeconds(1), processorTestDriver));
        dao.schedule(createTestTaskAttempt("afterWindowKey", "afterWindowValue", now.plusSeconds(2), processorTestDriver));


        // Advance stream time by 1 second
        processorTestDriver.getRetryPunctuator().punctuate(now.toInstant().toEpochMilli() + 1000L);

        assertActionReceivedCalls(processorTestDriver, Arrays.asList(
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
        assertEquals(500L, processorTestDriver.getContext().scheduledPunctuators().get(0).getIntervalMs());
    }


    @ParameterizedTest
    @DisplayName("It deletes an attempt from the attempts store once that attempt has been executed")
    @MethodSource("retryableProcessorDriverProvider")
    void testRetryDeletionOnSuccess(RetryableProcessorTestDriver<String, String> processorTestDriver){
        TaskAttemptsDAO taskAttemptsDAO = processorTestDriver.getTaskAttemptsDAO();
        assertEquals(0, processorTestDriver.getCountOfScheduledTaskAttempts());

        // Add an attempt to the store
        TaskAttempt testAttempt = createTestTaskAttempt("key", "value", ZonedDateTime.now(), processorTestDriver);
        taskAttemptsDAO.schedule(testAttempt);
        assertEquals(1, processorTestDriver.getCountOfScheduledTaskAttempts());

        // Execute the attempt, then assert that the attempt is no longer in the store
        KeyValue<Long, TaskAttempt> existingAttempt = processorTestDriver.getScheduledTaskAttempts().get(0);
        processorTestDriver.getRetryPunctuator().punctuate(testAttempt.getTimeOfNextAttempt().toInstant().toEpochMilli());
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
            assertEquals(0, processorTestDriver.getCountOfScheduledTaskAttempts());
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

            assertEquals(1, processorTestDriver.getCountOfScheduledTaskAttempts());
            assertAttemptForSameMessage("key", "value",
                                        processorTestDriver.getInputTopicName(),
                                        processorTestDriver.getScheduledTaskAttempts().get(0),
                                        processorTestDriver.getDefaultKeySerde().deserializer(),
                                        processorTestDriver.getDefaultValueSerde().deserializer());

        }


        @Test
        @DisplayName("It schedules another retry via the attempts store if an attempt is executed and throws a RetryableException")
        void testRetryRetryOnRetryableException(){
            TaskAttemptsDAO taskAttemptsDAO = processorTestDriver.getTaskAttemptsDAO();
            assertEquals(0, processorTestDriver.getCountOfScheduledTaskAttempts());

            // Add an attempt to the store
            ZonedDateTime timeOfFirstAttempt = ZonedDateTime.now();
            TaskAttempt testAttempt = createTestTaskAttempt("key", "value", timeOfFirstAttempt, processorTestDriver);
            taskAttemptsDAO.schedule(testAttempt);
            Integer previousAttemptsCount = testAttempt.getAttemptsCount();

            // Execute retry
            processorTestDriver.getRetryPunctuator().punctuate(timeOfFirstAttempt.toInstant().toEpochMilli());

            // Assert a new attempt has been scheduled
            List<KeyValue<Long, TaskAttempt>> scheduledAttempts = processorTestDriver.getScheduledTaskAttempts();
            assertEquals(1, scheduledAttempts.size());
            assertTrue(scheduledAttempts.get(0).key > timeOfFirstAttempt.toInstant().toEpochMilli());
            assertTrue(scheduledAttempts.get(0).value.getAttemptsCount() > previousAttemptsCount);

            assertAttemptForSameMessage("key", "value",
                    processorTestDriver.getInputTopicName(),
                    processorTestDriver.getScheduledTaskAttempts().get(0),
                    processorTestDriver.getDefaultKeySerde().deserializer(),
                    processorTestDriver.getDefaultValueSerde().deserializer());
        }

        @Test
        @DisplayName("It treats a job that has exhausted it's retries as having thrown a FailableException")
        void testRetryExhaustionException() throws IOException {
            // Use the KeyValueStore directly instead of DAO in order to schedule a task attempt that has exhausted attempts
            KeyValueStore<Long, TaskAttemptsCollection> attemptsStore = processorTestDriver.getAttemptStore();
            assertEquals(0, processorTestDriver.getCountOfScheduledTaskAttempts());

            // Create an attempt, advance it to it's final attempt, then add it to the store
            TaskAttempt testAttempt = createTestTaskAttempt("key", "value", ZonedDateTime.now(), processorTestDriver);
            while (!testAttempt.hasExhaustedRetries()){
                testAttempt.prepareForNextAttempt();
            }
            TaskAttemptsCollection collection = new TaskAttemptsCollection();
            collection.add(testAttempt);
            attemptsStore.put(testAttempt.getTimeOfNextAttempt().toInstant().toEpochMilli(), collection);

            // Execute retry
            processorTestDriver.getRetryPunctuator().punctuate(testAttempt.getTimeOfNextAttempt().toInstant().toEpochMilli());

            // No new attempt should be scheduled
            assertEquals(0, processorTestDriver.getCountOfScheduledTaskAttempts());

            // The Dead Letter Topic should receive a message
            assertMessageForwardedToDLT(processorTestDriver, testAttempt.getTimeReceived().toInstant().toEpochMilli(), 11);
        }


        @Test
        @DisplayName("It logs an WARN that an retryable error has occurred")
        void testLogging(){
            processorTestDriver.pipeInput("key", "value");

            assertEquals(1, logAppender.list.size());
            assertEquals(Level.WARN, logAppender.list.get(0).getLevel());
            assertEquals(
                    "A Retryable Error has occurred while processing the following message"
                    + "\n\tAttempt Number:\t1"
                    + "\n\tTopic:\t\t" + processorTestDriver.getInputTopicName()
                    + "\n\tKey:\t\tkey"
                    + "\n\tError:\t\t" + processorTestDriver.getAction().getException().toString(),
                    logAppender.list.get(0).getMessage()
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
            assertEquals(0, processorTestDriver.getCountOfScheduledTaskAttempts());
            processorTestDriver.pipeInput("key", "value");
            assertEquals(0, processorTestDriver.getCountOfScheduledTaskAttempts());
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

            assertEquals(1, logAppender.list.size());
            assertEquals(Level.ERROR, logAppender.list.get(0).getLevel());
            assertEquals(
                    "A Non-Retryable Error has occurred while processing the following message"
                    + "\n\tTopic:\t\t" + processorTestDriver.getInputTopicName()
                    + "\n\tKey:\t\tkey"
                    + "\n\tError:\t\t" + processorTestDriver.getAction().getException().toString(),
                    logAppender.list.get(0).getMessage()
                    );
        }
    }


    private void assertAttemptForSameMessage(String key, String value, String topicName, KeyValue<Long, TaskAttempt> savedAttempt, Deserializer<String> keyDerializer, Deserializer<String> valueDerializer  ){
        assertEquals(key, keyDerializer.deserialize(topicName, savedAttempt.value.getMessage().keyBytes));
        assertEquals(value, valueDerializer.deserialize(topicName, savedAttempt.value.getMessage().valueBytes));
    }

    /*
     * Assert that the processor sent a message to the Dead Letter Topic
     */
    private void assertMessageForwardedToDLT(RetryableProcessorTestDriver<String, String> processorTestDriver, Long timeOfReceipt, Integer attemptCount) throws IOException{
        List<MockProcessorContext.CapturedForward> forwarded = processorTestDriver.getContext().forwarded(processorTestDriver.getDeadLetterNodeName());
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


    /*
     * Assert that the mock action received calls with the specified arguments. This is not order dependent.
     */
    private void assertActionReceivedCalls(RetryableProcessorTestDriver<String, String> processorTestDriver, List<Pair<String, String>> parameters){
        assertEquals(parameters.size(), processorTestDriver.getAction().getReceivedParameters().size());
        parameters.forEach(parameter -> {
            assertTrue(processorTestDriver.getAction().getReceivedParameters().contains(parameter));
        });
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
