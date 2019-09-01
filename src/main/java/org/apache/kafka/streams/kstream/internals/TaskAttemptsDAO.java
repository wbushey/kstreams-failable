package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.time.ZonedDateTime;

public class TaskAttemptsDAO {
    private KeyValueStore<Long, TaskAttempt> attemptsStore;

    public TaskAttemptsDAO(KeyValueStore<Long, TaskAttempt> attemptsStore){
        this.attemptsStore = attemptsStore;
    }

    public void schedule(TaskAttempt attempt){
        this.attemptsStore.put(attempt.getTimeOfNextAttempt().toInstant().toEpochMilli(), attempt);
    }

    public void unschedule(TaskAttempt attempt){
        this.attemptsStore.delete(attempt.getTimeOfNextAttempt().toInstant().toEpochMilli());
    }

    public KeyValueIterator<Long, TaskAttempt> getAllTaskAttemptsScheduledBefore(long time){
        return this.attemptsStore.range(0L, time);
    }
}
