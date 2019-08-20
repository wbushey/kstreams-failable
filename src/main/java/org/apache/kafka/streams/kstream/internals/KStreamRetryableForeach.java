package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class KStreamRetryableForeach<K, V> implements ProcessorSupplier<K, V> {

    private final RetryableForeachAction<? super K, ? super V> action;
    private final String tasksStoreName;

    KStreamRetryableForeach(String tasksStoreName, final RetryableForeachAction<? super K, ? super V> action){
        this.tasksStoreName = tasksStoreName;
        this.action = action;
    }

    @Override
    public Processor<K, V> get() { return new RetryableKStreamRetryableForeachProcessor(); }

    private class RetryableKStreamRetryableForeachProcessor extends AbstractProcessor<K, V> {
        private ProcessorContext context;
        private KeyValueStore<Long, TaskAttempt> tasksStore;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context){
            this.context = context;

            this.tasksStore = (KeyValueStore) context.getStateStore(tasksStoreName);
        }

        @Override
        public void process(final K key, final V value){
            try {
                action.apply(key, value);
            } catch (RetryableKStream.RetryableException e) {
                TaskAttempt taskAttempt = new TaskAttempt(context.topic(), getBytesOfKey(key), getBytesOfValue(value));
                tasksStore.put(taskAttempt.getTimeOfNextAttempt().toInstant().toEpochMilli(), taskAttempt);
            } catch (RetryableKStream.FailableException e) {
                e.printStackTrace();
            }
        }

        @SuppressWarnings("unchecked")
        private byte[] getBytesOfKey(K key){
            final String topic = context.topic();
            Serde keySerde = context.keySerde();
            return keySerde.serializer().serialize(topic, key);
        }

        @SuppressWarnings("unchecked")
        private K getKeyFromBytes(byte[] keyBytes){
            final String topic = context.topic();
            Serde keySerde = context.keySerde();
            return (K)keySerde.deserializer().deserialize(topic, keyBytes);
        }

        @SuppressWarnings("unchecked")
        private byte[] getBytesOfValue(V value){
            final String topic = context.topic();
            Serde valueSerde = context.valueSerde();
            return valueSerde.serializer().serialize(topic, value);
        }

        @SuppressWarnings("unchecked")
        private V getValueFromBytes(byte[] valueBytes){
            final String topic = context.topic();
            Serde valueSerde = context.valueSerde();
            return (V)valueSerde.deserializer().deserialize(topic, valueBytes);
        }

    }
}
