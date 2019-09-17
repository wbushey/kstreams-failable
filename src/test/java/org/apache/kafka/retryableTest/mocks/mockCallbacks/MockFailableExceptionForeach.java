package org.apache.kafka.retryableTest.mocks.mockCallbacks;

import org.apache.kafka.retryableTest.Pair;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.kstream.internals.RetryableForeachAction;

import java.util.LinkedList;
import java.util.List;

public class MockFailableExceptionForeach<K, V> implements MockCallback<K, V> {
    private final List<Pair> receivedRecords = new LinkedList<>();
    private final RetryableKStream.FailableException exception = new RetryableKStream.FailableException("Testing Happened");
    private final RetryableForeachAction<K, V> callback = (key, value) -> {
        receivedRecords.add(new Pair<>(key, value));
        throw exception;
    };

    @Override
    public RetryableForeachAction<K, V> getCallback() { return callback; }

    @Override
    public List<Pair> getReceivedParameters() { return receivedRecords; }

    @Override
    public Exception getException(){ return exception; }
}
