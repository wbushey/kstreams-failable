package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.state.KeyValueStore;

public interface RetryableTestDriver<K, V> {

    /**
     * @return The name of the input topic
     */
    String getInputTopicName();


    /**
     * @return The default Key Serde
     */
    Serde<K> getDefaultKeySerde();

    /**
     * @return The default Value Serde
     */
    Serde<V> getDefaultValueSerde();

    /**
     * @return StateStore for task attempts
     */
    KeyValueStore<Long, TaskAttempt> getAttemptStore();

    /**
     * Send a message to the input topic
     * @param key   Key of the message to send
     * @param value Value of the message to send
     */
    void pipeInput(K key, V value);
}
