package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.internals.serdes.TaskAttemptSerde;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.LinkedList;
import java.time.Duration;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.*;

class TaskAttemptsDAOTest {
    private static final String DEAFULT_TEST_ATTEMPTS_STORE_NAME = "testAttemptsStore";
    private static final String DEAFULT_TEST_TOPIC_NAME = "testTopic";
    private KeyValueStore<Long, TaskAttempt> attemptsStore;
    private TaskAttemptsDAO subject;

    @BeforeEach
    void setUp(){
        final MockProcessorContext mockContext = new MockProcessorContext();
        this.attemptsStore = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(DEAFULT_TEST_ATTEMPTS_STORE_NAME), Serdes.Long(), new TaskAttemptSerde())
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

        KeyValueIterator<Long, TaskAttempt> retrievedAttemptsItr = subject.getAllTaskAttemptsScheduledBefore(timeToGetTaskAttempsAt.toInstant().toEpochMilli());
        List<TaskAttempt> retrievedAttempts = new LinkedList<>();
        retrievedAttemptsItr.forEachRemaining(pair -> retrievedAttempts.add(pair.value));
        assertEquals(4, retrievedAttempts.size());
        assertTrue(retrievedAttempts.contains(veryDelayedAttempt));
        assertTrue(retrievedAttempts.contains(delayedAttempt));
        assertTrue(retrievedAttempts.contains(withinSecondAttempt));
        assertTrue(retrievedAttempts.contains(exactTimeAttempt));
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


    @DisplayName("It can unschedule a task attempt from the store")
    @Test
    void unscheduleTaskAttemptFromAttemptStore(){
        TaskAttempt attempt = createTestTaskAttempt("key", "value");
        attemptsStore.put(attempt.getTimeOfNextAttempt().toInstant().toEpochMilli(), attempt);
        assertEquals(1, attemptsStore.approximateNumEntries());
        subject.unschedule(attempt);
        assertEquals(0, attemptsStore.approximateNumEntries());
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
