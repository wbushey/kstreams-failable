package org.apache.kafka.failableTestSupport.FailableTestDrivers;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.TaskAttemptsStore.TaskAttemptsDAO;
import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.state.KeyValueStore;

public interface FailableTestDriver<K, V> {

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
    KeyValueStore<Long, TaskAttemptsCollection> getAttemptStore();

    /**
     * @return TaskAttemptsDAO for the Driver's StateStore
     */
    TaskAttemptsDAO getTaskAttemptsDAO();

    /**
     * Send a message to the input topic
     * @param key   Key of the message to send
     * @param value Value of the message to send
     */
    void pipeInput(K key, V value);
}
