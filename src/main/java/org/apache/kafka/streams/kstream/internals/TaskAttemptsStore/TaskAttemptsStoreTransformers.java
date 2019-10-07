package org.apache.kafka.streams.kstream.internals.TaskAttemptsStore;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TaskAttemptsStoreTransformers {
    /**
     * @param tacIterator Iterator to flatten and streamify
     * @return Stream of items in this iterator, flattened
     */
    public static Stream<KeyValue<Long, TaskAttempt>> flattenedStreamFor(Iterable<KeyValue<Long, TaskAttemptsCollection>> tacIterator){
        // Convert Iterable into a Stream
        // Flatmap will turn each item in each Set into an item in the resulting stream
        // Within the Flatmap, the Set is also turned into a stream, and each TaskAttempt is mapped to a KeyValue of timestamp, TaskAttempt
        return StreamSupport.stream(tacIterator.spliterator(), false)
                .flatMap(kv -> StreamSupport.stream(kv.value.spliterator(), false)
                        .map(taskAttempt -> new KeyValue<>(kv.key, taskAttempt)));
    }

    /**
     * Convert the provided Iterator into an Iterable, which can be converted into a Stream
     * @param iterator Iterator to convert
     * @return Iterable representing the provided Iterator
     */
    public static Iterable<KeyValue<Long, TaskAttemptsCollection>> iterableFor(KeyValueIterator<Long, TaskAttemptsCollection> iterator){
        return () -> iterator;
    }


    public static Iterator<KeyValue<Long, TaskAttempt>> flattenedIteratorFor(Iterable<KeyValue<Long, TaskAttemptsCollection>> tacIterator){
        return flattenedStreamFor(tacIterator).iterator();
    }

}
