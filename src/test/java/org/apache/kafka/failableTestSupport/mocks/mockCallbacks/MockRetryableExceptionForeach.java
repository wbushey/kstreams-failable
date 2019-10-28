package org.apache.kafka.failableTestSupport.mocks.mockCallbacks;

import org.apache.kafka.failableTestSupport.Pair;
import org.apache.kafka.streams.kstream.FailableKStream;
import org.apache.kafka.streams.kstream.internals.FailableForeachAction;

import java.util.LinkedList;
import java.util.List;

public class MockRetryableExceptionForeach<K, V> implements MockCallback<K, V> {
    private final List<Pair> receivedRecords = new LinkedList<>();
    private final FailableKStream.RetryableException exception = new FailableKStream.RetryableException("Testing Happened");
    private final FailableForeachAction<K, V> callback = (key, value) -> {
        receivedRecords.add(new Pair<>(key, value));
        throw exception;
    };

    @Override
    public FailableForeachAction<K, V> getCallback() { return callback; }

    @Override
    public List<Pair> getReceivedParameters() { return receivedRecords; }


    @Override
    public Exception getException(){ return exception; }
}
