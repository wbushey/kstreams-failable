package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.kstream.internals.serialization.serdes.TaskAttemptsCollectionSerde;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.time.Duration;
import java.time.ZonedDateTime;

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
        ZonedDateTime timeToGetTaskAttempsAt = now.plus(Duration.ofMillis(100));
        assertEquals(0, attemptsStore.approximateNumEntries());

        TaskAttempt veryDelayedAttempt = createTestTaskAttempt("key", "value");
        veryDelayedAttempt.setTimeOfNextAttempt(timeToGetTaskAttempsAt.minus(Duration.ofDays(5)));
        subject.schedule(veryDelayedAttempt);

        TaskAttempt delayedAttempt = createTestTaskAttempt("key", "value");
        delayedAttempt.setTimeOfNextAttempt(timeToGetTaskAttempsAt.minus(Duration.ofMinutes(5)));
        subject.schedule(delayedAttempt);

        TaskAttempt withinSecondAttempt = createTestTaskAttempt("key", "value");
        withinSecondAttempt.setTimeOfNextAttempt(timeToGetTaskAttempsAt.minus(Duration.ofMillis(400)));
        subject.schedule(withinSecondAttempt);

        TaskAttempt exactTimeAttempt = createTestTaskAttempt("key", "value");
        exactTimeAttempt.setTimeOfNextAttempt(timeToGetTaskAttempsAt);
        subject.schedule(exactTimeAttempt);

        TaskAttempt afterTimeAttempt = createTestTaskAttempt("key", "value");
        afterTimeAttempt.setTimeOfNextAttempt(timeToGetTaskAttempsAt.plus(Duration.ofMillis(1)));
        subject.schedule(afterTimeAttempt);

        TaskAttempt veryAfterTimeAttempt = createTestTaskAttempt("key", "value");
        veryAfterTimeAttempt.setTimeOfNextAttempt(timeToGetTaskAttempsAt.plus(Duration.ofDays(5)));
        subject.schedule(veryAfterTimeAttempt);

        assertEquals(6, attemptsStore.approximateNumEntries());

        assertTaskAttemptsScheduledBeforeTime(timeToGetTaskAttempsAt,
                                              Arrays.asList(veryDelayedAttempt, delayedAttempt, withinSecondAttempt, exactTimeAttempt));
    }

    @DisplayName("It uses the TaskAttempt's TimeOfNextAttempt to schedule the task in the attempts store")
    @Test
    void usesTimeOfNextAttemptToScheduleInAttemptStore(){
        assertEquals(0, attemptsStore.approximateNumEntries());
        TaskAttempt attempt = createTestTaskAttempt("key", "value");
        subject.schedule(attempt);
        assertEquals(1, attemptsStore.approximateNumEntries());
        assertEquals(attempt, attemptsStore.get(attempt.getTimeOfNextAttempt().toInstant().toEpochMilli()));
    }

    @DisplayName("It throws a FailableException when attempting to schedule a task that has exhausted it's attempts.")
    @Test()
    void schedulingAnExhaustedTaskAttemptThrowsAFailableException(){
        TaskAttempt attempt = createTestTaskAttempt("key", "value");
        while (!attempt.hasExhaustedRetries()){
            attempt.prepareForNextAttempt();
        }

        assertThrows(RetryableKStream.RetriesExhaustedException.class, () -> {
            subject.schedule(attempt);
        });
    }


    @DisplayName("It can unschedule a task attempt from the store")
    @Test
    void unscheduleTaskAttemptFromAttemptStore(){
        TaskAttempt attempt = createTestTaskAttempt("key", "value");
        TaskAttemptsCollection attempts = new TaskAttemptsCollection();
        attempts.add(attempt);
        attemptsStore.put(attempt.getTimeOfNextAttempt().toInstant().toEpochMilli(), attempts);
        assertEquals(1, attemptsStore.approximateNumEntries());
        subject.unschedule(attempt);
        assertEquals(0, attemptsStore.approximateNumEntries());
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

        assertTaskAttemptsScheduledBeforeTime(aTime, Arrays.asList(attempt1, attempt2, attempt3));
    }

    private void assertTaskAttemptsScheduledBeforeTime(ZonedDateTime time, List<TaskAttempt> expectedAttempts){
        Iterator<KeyValue<Long, TaskAttempt>> retrievedAttemptsItr = subject.getAllTaskAttemptsScheduledBefore(time.toInstant().toEpochMilli());
        List<TaskAttempt> retrievedAttempts = new LinkedList<>();
        retrievedAttemptsItr.forEachRemaining(pair -> retrievedAttempts.add(pair.value));
        assertEquals(expectedAttempts.size(), retrievedAttempts.size(), "Retrieved scheduled TaskAttempts count different than expected scheduled TaskAttempts");
        expectedAttempts.forEach(attempt -> assertTrue(retrievedAttempts.contains(attempt)));
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

}
