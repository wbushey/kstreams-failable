package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.RetryableKStream;

public interface RetryableForeachAction<K, V> {

    /**
     * Perform an action for each record of a stream.
     *
     * When an error is encountered that should be retried, throw RetryableKStream.RetryableException.
     * When an error is encountered that should not be retried, throw RetryableKStream.FailableException.
     *
     * @param key   the key of the record
     * @param value the value of the record
     */
    void apply(final K key, final V value) throws RetryableKStream.RetryableException, RetryableKStream.FailableException;
}
