package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.kstream.internals.serialization.serdes.TaskAttemptsCollectionSerde;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class StoreBuilders {

    public enum BackingStore {
        PERSISTENT, IN_MEMORY
    }

    /**
     * @param taskAttemptsStoreName  Name that will be used to fetch the state store
     * @param backingStore           Type of backing store. Either PERSISTENT or IN_MEMORY
     * @return                      A StoreBuilder for a state store to store TaskAttempts
     */
    public static StoreBuilder<KeyValueStore<Long, TaskAttemptsCollection>> getTaskAttemptsStoreBuilder(String taskAttemptsStoreName, BackingStore backingStore){
        return Stores.keyValueStoreBuilder(
                getKeyValueBytesStoreSupplier(taskAttemptsStoreName, backingStore),
                Serdes.Long(), new TaskAttemptsCollectionSerde());
    }

    private static KeyValueBytesStoreSupplier getKeyValueBytesStoreSupplier(String taskAttemptsStoreName, BackingStore backingStore){
        KeyValueBytesStoreSupplier storeSupplier = null;

        switch(backingStore){
            case PERSISTENT:    storeSupplier = Stores.persistentKeyValueStore(taskAttemptsStoreName);
                break;
            case IN_MEMORY:     storeSupplier = Stores.inMemoryKeyValueStore(taskAttemptsStoreName);
                break;
        }

        return storeSupplier;
    }

}
