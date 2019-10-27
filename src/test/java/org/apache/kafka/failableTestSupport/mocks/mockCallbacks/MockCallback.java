package org.apache.kafka.failableTestSupport.mocks.mockCallbacks;

import org.apache.kafka.failableTestSupport.Pair;
import org.apache.kafka.streams.kstream.internals.FailableForeachAction;

import java.util.List;

public interface MockCallback<K, V> {
    /**
     * @return The callback to provide to a stream processor.
     */
    public FailableForeachAction<K, V> getCallback();

    /**
     * @return A list of parameters received by the callback.
     */
    public List<Pair> getReceivedParameters();


    /**
     * @return The exception that is thrown by this mock callback. Will return null if no exception is thrown.
     */
    public Exception getException();
}
