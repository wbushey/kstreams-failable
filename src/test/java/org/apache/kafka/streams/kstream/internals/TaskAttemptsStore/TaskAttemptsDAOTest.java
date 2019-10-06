package org.apache.kafka.streams.kstream.internals.TaskAttemptsStore;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.kstream.internals.StoreBuilders;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Arrays;

import static org.apache.kafka.retryableTest.AttemptStoreAssertions.expect;
import static org.apache.kafka.retryableTest.TaskAttemptsStoreTestAccess.access;
import static org.junit.jupiter.api.Assertions.*;

class TaskAttemptsDAOTest {
    private static final String DEAFULT_TEST_ATTEMPTS_STORE_NAME = "testAttemptsStore";
    private static final String DEAFULT_TEST_TOPIC_NAME = "testTopic";
    private KeyValueStore<Long, TaskAttemptsCollection> attemptsStore;
    private TaskAttemptsDAO subject;

    @BeforeEach
    void setUp(){
        final MockProcessorContext mockContext = new MockProcessorContext();
        this.attemptsStore = StoreBuilders.getTaskAttemptsStoreBuilder(DEAFULT_TEST_ATTEMPTS_STORE_NAME, StoreBuilders.BackingStore.IN_MEMORY)
                .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                .build();

        attemptsStore.init(mockContext, attemptsStore);
        mockContext.register(attemptsStore, null);

        this.subject = new TaskAttemptsDAO(this.attemptsStore);
    }

    @DisplayName("It can retrieve all TaskAttempts scheduled up until a provided time")
    @Test
    void canRetrieveAllTaskAttemptsScheduledBeforeOrAtATime(){
        ZonedDateTime now = ZonedDateTime.now();
        ZonedDateTime timeToGetTaskAttemptsAt = now.plus(Duration.ofMillis(100));
        expect(attemptsStore).toBeEmpty();

        // Create and schedule attempts at various times before and up to the time we will query at
        TaskAttempt veryDelayedAttempt = createTaskAttemptAndScheduleAt("key", "value", timeToGetTaskAttemptsAt.minus(Duration.ofDays(5)));
        TaskAttempt delayedAttempt = createTaskAttemptAndScheduleAt("key", "value", timeToGetTaskAttemptsAt.minus(Duration.ofMinutes(5)));
        TaskAttempt withinSecondAttempt = createTaskAttemptAndScheduleAt("key", "value", timeToGetTaskAttemptsAt.minus(Duration.ofMillis(400)));
        TaskAttempt exactTimeAttempt = createTaskAttemptAndScheduleAt("key", "value", timeToGetTaskAttemptsAt);

        // Create and schedule a couple of tasks at times after the time we will query at
        createTaskAttemptAndScheduleAt("key", "value", timeToGetTaskAttemptsAt.plus(Duration.ofMillis(1)));
        createTaskAttemptAndScheduleAt("key", "value", timeToGetTaskAttemptsAt.plus(Duration.ofDays(5)));

        expect(attemptsStore).toHaveStoredTaskAttemptsCountOf(6);
        expect(attemptsStore).toHaveTaskAttemptsBeforeTime(Arrays.asList(veryDelayedAttempt, delayedAttempt, withinSecondAttempt, exactTimeAttempt),
                                                            timeToGetTaskAttemptsAt.toInstant().toEpochMilli());
    }

    @DisplayName("It uses the TaskAttempt's TimeOfNextAttempt to schedule the task in the attempts store")
    @Test
    void usesTimeOfNextAttemptToScheduleInAttemptStore(){
        expect(attemptsStore).toBeEmpty();
        TaskAttempt attempt = createTestTaskAttempt("key", "value");
        subject.schedule(attempt);
        expect(attemptsStore).toHaveStoredTaskAttemptsCountOf(1);

        Long timeOfAttempt = attempt.getTimeOfNextAttempt().toInstant().toEpochMilli();
        expect(attemptsStore).toHaveTaskAttemptAtTime(attempt, timeOfAttempt);
    }

    @DisplayName("It throws a FailableException when attempting to schedule a task that has exhausted it's attempts.")
    @Test()
    void schedulingAnExhaustedTaskAttemptThrowsAFailableException(){
        TaskAttempt attempt = createTestTaskAttempt("key", "value");
        while (!attempt.hasExhaustedRetries()){
            attempt.prepareForNextAttempt();
        }

        assertThrows(RetryableKStream.RetriesExhaustedException.class, () -> subject.schedule(attempt));
    }


    @DisplayName("It can unschedule a task attempt from the store")
    @Test
    void unscheduleTaskAttemptFromAttemptStore(){
        TaskAttempt attempt = createTestTaskAttempt("key", "value");
        access(attemptsStore).addAttemptAt(attempt.getTimeOfNextAttempt().toInstant().toEpochMilli(), attempt);
        assertEquals(1, access(attemptsStore).getStoredTaskAttemptsCount());
        subject.unschedule(attempt);
        assertEquals(0, access(attemptsStore).getStoredTaskAttemptsCount());
    }

    @DisplayName("It can schedule multiple task attempts at the same time")
    @Test
    void multipleAttemptsAtSameTime(){
        ZonedDateTime aTime = ZonedDateTime.now();
        TaskAttempt attempt1 = createTestTaskAttempt("key1", "value1");
        TaskAttempt attempt2 = createTestTaskAttempt("key2", "value2");
        TaskAttempt attempt3 = createTestTaskAttempt("key3", "value3");
        attempt1.setTimeOfNextAttempt(aTime);
        attempt2.setTimeOfNextAttempt(aTime);
        attempt3.setTimeOfNextAttempt(aTime);

        subject.schedule(attempt1);
        subject.schedule(attempt2);
        subject.schedule(attempt3);

        expect(attemptsStore).toHaveTaskAttemptsBeforeTime(Arrays.asList(attempt1, attempt2, attempt3), aTime);
    }

    private TaskAttempt createTestTaskAttempt(String key, String value){
        final String topicName = DEAFULT_TEST_TOPIC_NAME;
        final Serde<String> stringSerde = Serdes.String();
        return new TaskAttempt(
                topicName,
                stringSerde.serializer().serialize(topicName, key),
                stringSerde.serializer().serialize(topicName, value)
        );
    }

    private TaskAttempt createTaskAttemptAndScheduleAt(String key, String value, ZonedDateTime time){
        TaskAttempt attempt = createTestTaskAttempt(key, value);
        attempt.setTimeOfNextAttempt(time);
        subject.schedule(attempt);
        return attempt;
    }

}
