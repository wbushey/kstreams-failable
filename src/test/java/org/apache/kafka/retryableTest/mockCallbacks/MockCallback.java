package org.apache.kafka.retryableTest.mockCallbacks;

import org.apache.kafka.retryableTest.Pair;
import org.apache.kafka.streams.kstream.internals.RetryableForeachAction;

import java.util.List;

public interface MockCallback<K, V> {
    /**
     * Returns the callback to provide to a stream processor.
     * @return
     */
    public RetryableForeachAction<K, V> getCallback();

    /**
     * Returns a list of parameters received by the callback.
     * @return
     */
    public List<Pair> getReceivedParameters();
}
