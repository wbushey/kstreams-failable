package org.apache.kafka.streams.kstream.internals.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A Collection of TaskAttempts. This encapsulates a Java collection and enables type checking to occur in
 * Serialization/Deserialization functions.
 */
public class TaskAttemptsCollection implements Collection<TaskAttempt>, Serializable {
    // Chosen implementation of Collection must also implement Serializable
    private final Collection<TaskAttempt> collection = new ArrayList<>();

    @Override
    public int size() {
        return collection.size();
    }

    @Override
    public boolean isEmpty() {
        return collection.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return collection.contains(o);
    }

    @Override
    public Iterator<TaskAttempt> iterator() {
        return collection.iterator();
    }

    @Override
    public void forEach(Consumer<? super TaskAttempt> action) {
        collection.forEach(action);
    }

    @Override
    public Object[] toArray() {
        return collection.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return collection.toArray(a);
    }

    @Override
    public boolean add(TaskAttempt taskAttempt) {
        Boolean result = collection.add(taskAttempt);
        return result;
    }

    @Override
    public boolean remove(Object o) {
        return collection.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return collection.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends TaskAttempt> c) {
        return collection.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return collection.removeAll(c);
    }

    @Override
    public boolean removeIf(Predicate<? super TaskAttempt> filter) {
        return collection.removeIf(filter);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return collection.retainAll(c);
    }

    @Override
    public void clear() {
        collection.clear();
    }

    @Override
    public Spliterator<TaskAttempt> spliterator() {
        return collection.spliterator();
    }

    @Override
    public Stream<TaskAttempt> stream() {
        return collection.stream();
    }

    @Override
    public Stream<TaskAttempt> parallelStream() {
        return collection.parallelStream();
    }
}
