package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.state.KeyValueStore;

public interface RetryableTestDriver<K, V> {

    /**
     * @return The name of the input topic
     */
    public String getInputTopicName();


    /**
     * @return The default Key Serde
     */
    public Serde<K> getDefaultKeySerde();

    /**
     * @return The default Value Serde
     */
    public Serde<V> getDefaultValueSerde();

    /**
     * @return StateStore for task attempts
     */
    public KeyValueStore<Long, TaskAttempt> getAttemptStore();
}
