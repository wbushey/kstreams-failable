package org.apache.kafka.streams.kstream.internals.TaskAttemptsStore;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Iterator;

import static org.apache.kafka.streams.kstream.internals.TaskAttemptsStore.TaskAttemptsStoreTransformers.flattenedIteratorFor;
import static org.apache.kafka.streams.kstream.internals.TaskAttemptsStore.TaskAttemptsStoreTransformers.iterableFor;

public class TaskAttemptsDAO {
    private KeyValueStore<Long, TaskAttemptsCollection> attemptsStore;



    public TaskAttemptsDAO(KeyValueStore<Long, TaskAttemptsCollection> attemptsStore){
        this.attemptsStore = attemptsStore;
    }

    /**
     * Schedules execution of a provided TaskAttempt.
     * The TaskAttempts' timeOfNextAttempt will be used to schedule when the attempt will be executed.
     *
     * @param attempt - The TaskAttempt to schedule execution of.
     * @throws RetryableKStream.RetriesExhaustedException - If an attempt is made to schedule a TaskAttempt that can no longer be attempted.
     */
    public void schedule(TaskAttempt attempt) throws RetryableKStream.RetriesExhaustedException {
        if (attempt.hasExhaustedRetries()){
            throw new RetryableKStream.RetriesExhaustedException("Retry attempts have been exhausted.");
        }
        insert(attempt);
    }

    private void insert(TaskAttempt attempt){
        Long scheduledTime = attempt.getTimeOfNextAttempt().toInstant().toEpochMilli();
        this.attemptsStore.putIfAbsent(scheduledTime, new TaskAttemptsCollection());
        TaskAttemptsCollection collection = this.attemptsStore.get(scheduledTime);
        collection.add(attempt);
        this.attemptsStore.put(scheduledTime, collection);
    }

    public void unschedule(TaskAttempt attempt){
        Long scheduledTime = attempt.getTimeOfNextAttempt().toInstant().toEpochMilli();
        TaskAttemptsCollection scheduledTimeStore = this.attemptsStore.get(scheduledTime);
        scheduledTimeStore.remove(attempt);
        if (scheduledTimeStore.isEmpty()){
            this.attemptsStore.delete(scheduledTime);
        }
    }

    public Iterator<KeyValue<Long, TaskAttempt>> getAllTaskAttemptsScheduledBefore(long time){
        return flattenedIteratorFor(iterableFor(this.attemptsStore.range(0L, time)));
    }

    public Iterator<KeyValue<Long, TaskAttempt>> getAllTaskAttempts(){
        return flattenedIteratorFor(iterableFor(this.attemptsStore.all()));
    }
}
