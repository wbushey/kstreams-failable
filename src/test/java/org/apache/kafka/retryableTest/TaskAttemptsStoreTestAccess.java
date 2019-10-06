package org.apache.kafka.retryableTest;

import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.state.KeyValueStore;

import static org.apache.kafka.streams.kstream.internals.TaskAttemptsStore.TaskAttemptsStoreAdapter.flattenedStreamFor;
import static org.apache.kafka.streams.kstream.internals.TaskAttemptsStore.TaskAttemptsStoreAdapter.iterableFor;

/**
 * Module of static functions for tests to use to directly query or manipulate a TaskAttempts store
 */
public class TaskAttemptsStoreTestAccess {

    public static Integer storedTaskAttemptsCount(KeyValueStore<Long, TaskAttemptsCollection> attemptsStore){
        flattenedStreamFor(iterableFor(attemptsStore.all()));
        return 0;
    }
}
