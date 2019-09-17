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
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
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
        long now = ZonedDateTime.now().toInstant().toEpochMilli();
        KeyValueStore<Long, TaskAttempt> attemptsStore = processorTestDriver.getAttemptStore();
        assertEquals(0, attemptsStore.approximateNumEntries());

        attemptsStore.put(now - Duration.ofDays(5).toMillis(), createTestTaskAttempt("veryOldKey", "veryOldValue", processorTestDriver));
        attemptsStore.put(now - 1000L, createTestTaskAttempt("recentOldKey1", "recentOldValue1", processorTestDriver));
        attemptsStore.put(now - 500L, createTestTaskAttempt("recentOldKey2", "recentOldValue2", processorTestDriver));
        attemptsStore.put(now + 500L, createTestTaskAttempt("scheduledInCurrentWindowKey1", "scheduleInCurrentWindowValue1", processorTestDriver));
        attemptsStore.put(now + 1000L, createTestTaskAttempt("scheduledInCurrentWindowKey2", "scheduleInCurrentWindowValue2", processorTestDriver));
        attemptsStore.put(now + 2000L, createTestTaskAttempt("afterWindowKey", "afterWindowValue", processorTestDriver));

        // Advance stream time by 1 second
        processorTestDriver.getRetryPunctuator().punctuate(now + 1000L);

        assertEquals(5, processorTestDriver.getAction().getReceivedParameters().size());
        assertTrue(processorTestDriver.getAction().getReceivedParameters().contains(new Pair<>("veryOldKey", "veryOldValue")));
        assertTrue(processorTestDriver.getAction().getReceivedParameters().contains(new Pair<>("recentOldKey1", "recentOldValue1")));
        assertTrue(processorTestDriver.getAction().getReceivedParameters().contains(new Pair<>("recentOldKey2", "recentOldValue2")));
        assertTrue(processorTestDriver.getAction().getReceivedParameters().contains(new Pair<>("scheduledInCurrentWindowKey1", "scheduleInCurrentWindowValue1")));
        assertTrue(processorTestDriver.getAction().getReceivedParameters().contains(new Pair<>("scheduledInCurrentWindowKey1", "scheduleInCurrentWindowValue1")));
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
        KeyValueStore<Long, TaskAttempt> attemptsStore = processorTestDriver.getAttemptStore();
        assertEquals(0, attemptsStore.approximateNumEntries());

        // Add an attempt to the store
        TaskAttempt testAttempt = createTestTaskAttempt("key", "value", processorTestDriver);
        Long timeOfAttempt = testAttempt.getTimeOfNextAttempt().toInstant().toEpochMilli();
        attemptsStore.put(timeOfAttempt, testAttempt);
        assertEquals(1, attemptsStore.approximateNumEntries());


        // Execute the attempt, then assert that the attempt is no longer in the store
        KeyValue<Long, TaskAttempt> existingAttempt = processorTestDriver.getScheduledTaskAttempts().get(0);
        processorTestDriver.getRetryPunctuator().punctuate(timeOfAttempt);
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
            assertEquals(0, processorTestDriver.getScheduledTaskAttempts().size());
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

            assertEquals(1, processorTestDriver.getScheduledTaskAttempts().size());
            assertAttemptForSameMessage("key", "value",
                                        processorTestDriver.getInputTopicName(),
                                        processorTestDriver.getScheduledTaskAttempts().get(0),
                                        processorTestDriver.getDefaultKeySerde().deserializer(),
                                        processorTestDriver.getDefaultValueSerde().deserializer());

        }


        @Test
        @DisplayName("It schedules another retry via the attempts store if an attempt is executed and throws a RetryableException")
        void testRetryRetryOnRetryableException(){
            KeyValueStore<Long, TaskAttempt> attemptsStore = processorTestDriver.getAttemptStore();
            assertEquals(0, attemptsStore.approximateNumEntries());

            // Add an attempt to the store
            TaskAttempt testAttempt = createTestTaskAttempt("key", "value", processorTestDriver);
            Long timeOfAttempt = testAttempt.getTimeOfNextAttempt().toInstant().toEpochMilli();
            Integer previousAttemptsCount = testAttempt.getAttemptsCount();
            attemptsStore.put(timeOfAttempt, testAttempt);

            // Execute retry
            processorTestDriver.getRetryPunctuator().punctuate(timeOfAttempt);

            // Assert a new attempt has been scheduled
            List<KeyValue<Long, TaskAttempt>> scheduledAttempts = processorTestDriver.getScheduledTaskAttempts();
            assertEquals(1, scheduledAttempts.size());
            assertTrue(scheduledAttempts.get(0).key > timeOfAttempt);
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
            KeyValueStore<Long, TaskAttempt> attemptsStore = processorTestDriver.getAttemptStore();
            assertEquals(0, attemptsStore.approximateNumEntries());

            // Create an attempt, advance it to it's final attempt, then add it to the store
            TaskAttempt testAttempt = createTestTaskAttempt("key", "value", processorTestDriver);
            while (!testAttempt.hasExhaustedRetries()){
                testAttempt.prepareForNextAttempt();
            }
            Long timeOfAttempt = testAttempt.getTimeOfNextAttempt().toInstant().toEpochMilli();
            attemptsStore.put(timeOfAttempt, testAttempt);

            // Execute retry
            processorTestDriver.getRetryPunctuator().punctuate(timeOfAttempt);

            // No new attempt should be scheduled
            assertEquals(0, processorTestDriver.getScheduledTaskAttempts().size());

            // The Dead Letter Topic should receive a message
            assertMessageForwardedToDLT(processorTestDriver, testAttempt.getTimeReceived().toInstant().toEpochMilli(), 11);
        }


        @Test
        @DisplayName("It logs an WARN that an retryable error has occurred")
        void testLogging(){
            ListAppender<ILoggingEvent> listAppender = getTestLogAppenender();

            processorTestDriver.pipeInput("key", "value");

            assertEquals(1, listAppender.list.size());
            assertEquals(Level.WARN, listAppender.list.get(0).getLevel());
            assertEquals(
                    "A Retryable Error has occurred while processing the following message"
                    + "\n\tAttempt Number:\t1"
                    + "\n\tTopic:\t\t" + processorTestDriver.getInputTopicName()
                    + "\n\tKey:\t\tkey"
                    + "\n\tError:\t\t" + processorTestDriver.getAction().getException().toString(),
                    listAppender.list.get(0).getMessage()
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
            assertEquals(0, processorTestDriver.getScheduledTaskAttempts().size());
            processorTestDriver.pipeInput("key", "value");
            assertEquals(0, processorTestDriver.getScheduledTaskAttempts().size());
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
            ListAppender<ILoggingEvent> listAppender = getTestLogAppenender();

            processorTestDriver.pipeInput("key", "value");

            assertEquals(1, listAppender.list.size());
            assertEquals(Level.ERROR, listAppender.list.get(0).getLevel());
            assertEquals(
                    "A Non-Retryable Error has occurred while processing the following message"
                    + "\n\tTopic:\t\t" + processorTestDriver.getInputTopicName()
                    + "\n\tKey:\t\tkey"
                    + "\n\tError:\t\t" + processorTestDriver.getAction().getException().toString(),
                    listAppender.list.get(0).getMessage()
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
                         new MockRetryableExceptionForeach<String, String>(),
                         new MockFailableExceptionForeach<String, String>())
                .map(mockCallback -> new RetryableProcessorTestDriver<>(mockCallback, createTopologyProps(), stringSerde, stringSerde));
    }

    // TODO Move this is an extention with Before/After hooks
    private static ListAppender<ILoggingEvent> getTestLogAppenender(){
        Logger foreachLogger = (Logger) LoggerFactory.getLogger(KStreamRetryableForeach.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        foreachLogger.addAppender(listAppender);
        return listAppender;
    }
}
