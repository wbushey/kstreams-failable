package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
        Long scheduledTime = attempt.getTimeOfNextAttempt().toInstant().toEpochMilli();
        this.attemptsStore.putIfAbsent(scheduledTime, new TaskAttemptsCollection());
        this.attemptsStore.get(scheduledTime).add(attempt);
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
        // Convert the Iterator provided by range into an Iterable, which can be converted into a Stream
        Iterable<KeyValue<Long, TaskAttemptsCollection>> iterableOfSetsOfScheduledTasks = () -> this.attemptsStore.range(0L, time);

        return flatten(iterableOfSetsOfScheduledTasks);
    }

    public Iterator<KeyValue<Long, TaskAttempt>> getAllTaskAttempts(){
        // Convert the Iterator provided by all into an Iterable, which can be converted into a Stream
        Iterable<KeyValue<Long, TaskAttemptsCollection>> iterableOfSetsOfScheduledTasks = () -> this.attemptsStore.all();

        return flatten(iterableOfSetsOfScheduledTasks);
    }

    private Iterator<KeyValue<Long, TaskAttempt>> flatten(Iterable<KeyValue<Long, TaskAttemptsCollection>> tacIterator){
        // Convert Iterable into a Stream
        // Flatmap will turn each item in each Set into an item in the resulting stream
        // Within the Flatmap, the Set is also turned into a stream, and each TaskAttempt is mapped to a KeyValue of timestamp, TaskAttempt
        // Finally, the stream is converted into an iterator
        return StreamSupport.stream(tacIterator.spliterator(), false)
                .flatMap(kv -> StreamSupport.stream(kv.value.spliterator(), false)
                        .map(taskAttempt -> new KeyValue<>(kv.key, taskAttempt)))
                .iterator();

    }
}
