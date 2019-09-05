package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueIterator;

public class TaskAttemptsDAO {
    private KeyValueStore<Long, TaskAttempt> attemptsStore;

    public TaskAttemptsDAO(KeyValueStore<Long, TaskAttempt> attemptsStore){
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
        this.attemptsStore.put(attempt.getTimeOfNextAttempt().toInstant().toEpochMilli(), attempt);
    }

    public void unschedule(TaskAttempt attempt){
        this.attemptsStore.delete(attempt.getTimeOfNextAttempt().toInstant().toEpochMilli());
    }

    public KeyValueIterator<Long, TaskAttempt> getAllTaskAttemptsScheduledBefore(long time){
        return this.attemptsStore.range(0L, time);
    }
}
