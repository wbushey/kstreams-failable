package org.apache.kafka.retryableTestSupport;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;

import static org.apache.kafka.streams.kstream.internals.TaskAttemptsStore.TaskAttemptsStoreTransformers.*;

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

    public List<KeyValue<Long, TaskAttempt>> getAllTaskAttempts(){
        List<KeyValue<Long, TaskAttempt>> scheduledTaskAttempts = new LinkedList<>();
        flattenedIteratorFor(iterableFor(attemptsStore.all())).forEachRemaining(scheduledTaskAttempts::add);
        return scheduledTaskAttempts;
    }

    public TaskAttemptsCollection getTasksAt(Long time){
        return attemptsStore.get(time);
    }

    public void addAttemptAt(ZonedDateTime time, TaskAttempt attempt){
        addAttemptAt(time.toInstant().toEpochMilli(), attempt);
    }

    public void addAttemptAt(Long time, TaskAttempt attempt){
        TaskAttemptsCollection attempts = new TaskAttemptsCollection();
        attempts.add(attempt);
        attemptsStore.put(time, attempts);
    }

    private final KeyValueStore<Long, TaskAttemptsCollection> attemptsStore;

    private TaskAttemptsStoreTestAccess(KeyValueStore<Long, TaskAttemptsCollection> attemptsStore){
        this.attemptsStore = attemptsStore;
    }
}
