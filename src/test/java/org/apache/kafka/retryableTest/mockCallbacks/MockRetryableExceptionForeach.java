package org.apache.kafka.retryableTest.mockCallbacks;

import org.apache.kafka.retryableTest.Pair;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.kstream.internals.RetryableForeachAction;

import java.util.LinkedList;
import java.util.List;

public class MockRetryableExceptionForeach<K, V> implements MockCallback<K, V> {
    private final List<Pair> receivedRecords = new LinkedList<>();
    private final RetryableForeachAction<K, V> callback = (key, value) -> {
        receivedRecords.add(new Pair<>(key, value));
        throw new RetryableKStream.RetryableException("Testing Happened");
    };

    @Override
    public RetryableForeachAction<K, V> getCallback() { return callback; }

    @Override
    public List<Pair> getReceivedParameters() { return receivedRecords; }
}
