package org.apache.kafka.retryableTest;

import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.state.KeyValueStore;

import static org.apache.kafka.streams.kstream.internals.TaskAttemptsStore.TaskAttemptsStoreAdapter.flattenedStreamFor;
import static org.apache.kafka.streams.kstream.internals.TaskAttemptsStore.TaskAttemptsStoreAdapter.iterableFor;

/**
 * Module of functions for tests to use to directly query or manipulate a TaskAttempts store
 */
public class TaskAttemptsStoreTestAccess {
    /**
     * Decorates a KeyValueStore with methods to query and modify that store
     * @param attemptsStore
     * @return
     */
    public static TaskAttemptsStoreTestAccess access(KeyValueStore<Long, TaskAttemptsCollection> attemptsStore){
        return new TaskAttemptsStoreTestAccess(attemptsStore);
    }

    public Integer getStoredTaskAttemptsCount(){
        return Math.toIntExact(
                flattenedStreamFor(iterableFor(attemptsStore.all()))
                .count()
        );
    }

    public TaskAttemptsCollection getTasksAt(Long time){
        return attemptsStore.get(time);
    }

    public void addAttemptAt(Long time, TaskAttempt attempt){
        TaskAttemptsCollection attempts = new TaskAttemptsCollection();
        attempts.add(attempt);
        attemptsStore.put(attempt.getTimeOfNextAttempt().toInstant().toEpochMilli(), attempts);
    }

    private final KeyValueStore<Long, TaskAttemptsCollection> attemptsStore;

    private TaskAttemptsStoreTestAccess(KeyValueStore<Long, TaskAttemptsCollection> attemptsStore){
        this.attemptsStore = attemptsStore;
    }
}
