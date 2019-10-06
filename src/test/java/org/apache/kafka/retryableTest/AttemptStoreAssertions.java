package org.apache.kafka.retryableTest;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.apache.kafka.retryableTest.TaskAttemptsStoreTestAccess.access;
import static org.apache.kafka.streams.kstream.internals.TaskAttemptsStore.TaskAttemptsStoreAdapter.iterableFor;
import static org.apache.kafka.streams.kstream.internals.TaskAttemptsStore.TaskAttemptsStoreAdapter.flattenedIteratorFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AttemptStoreAssertions {
    private final KeyValueStore<Long, TaskAttemptsCollection> attemptsStore;

    /**
     * Decorates a KeyValueStore with methods that assert expectations.
     * @param attemptsStore
     * @return
     */
    public static AttemptStoreAssertions expect(KeyValueStore<Long, TaskAttemptsCollection> attemptsStore){
        return new AttemptStoreAssertions(attemptsStore);
    }

    public void toBeEmpty(){
        assertEquals(0, access(attemptsStore).getStoredTaskAttemptsCount());
    }

    public void toHaveStoredTaskAttemptsCountOf(Integer expectedCount){
        assertEquals(expectedCount, access(attemptsStore).getStoredTaskAttemptsCount());
    }

    public void toHaveTaskAttemptAtTime(TaskAttempt attempt, Long time){
        assertTrue(attemptsStore.get(time).contains(attempt));
    }

    public void toHaveTaskAttemptsBeforeTime(List<TaskAttempt> expectedAttempts, ZonedDateTime time){
        toHaveTaskAttemptsBeforeTime(expectedAttempts, time.toInstant().toEpochMilli());
    }

    public void toHaveTaskAttemptsBeforeTime(List<TaskAttempt> expectedAttempts, Long time){
        Iterator<KeyValue<Long, TaskAttempt>> retrievedAttemptsItr =  flattenedIteratorFor(iterableFor(attemptsStore.range(0L, time)));
        List<TaskAttempt> retrievedAttempts = new LinkedList<>();
        retrievedAttemptsItr.forEachRemaining(pair -> retrievedAttempts.add(pair.value));
        assertEquals(expectedAttempts.size(), retrievedAttempts.size(), "Retrieved scheduled TaskAttempts count different than expected scheduled TaskAttempts");
        expectedAttempts.forEach(attempt -> assertTrue(retrievedAttempts.contains(attempt)));
    }

    private AttemptStoreAssertions(KeyValueStore<Long, TaskAttemptsCollection> attemptsStore){
        this.attemptsStore = attemptsStore;
    }

}
