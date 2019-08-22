package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

// TODO Add logging

public class KStreamRetryableForeach<K, V> implements ProcessorSupplier<K, V> {

    private static final Long ATTEMPTS_PUNCTUATE_INTERVAL_MS = 500L;
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
        private Long timeOfLastQuery = 0L;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context){
            this.context = context;

            this.tasksStore = (KeyValueStore) context.getStateStore(tasksStoreName);

            this.context.schedule(Duration.ofMillis(ATTEMPTS_PUNCTUATE_INTERVAL_MS), PunctuationType.WALL_CLOCK_TIME,
                    (timestamp) -> { performAttemptsScheduledFor(timestamp); });
        }

        @Override
        public void process(final K key, final V value){
            TaskAttempt attempt = new TaskAttempt(context.topic(), getBytesOfKey(key), getBytesOfValue(value));
            performAttempt(attempt);
        }

        private void performAttemptsScheduledFor(Long punctuateTimestamp){
            KeyValueIterator<Long, TaskAttempt> scheduledTasks = this.tasksStore.range(timeOfLastQuery, punctuateTimestamp);
            scheduledTasks.forEachRemaining(scheduledTask -> {
                this.tasksStore.delete(scheduledTask.key);
                performAttempt(scheduledTask.value);
            });
            timeOfLastQuery = punctuateTimestamp;
        }

        private void performAttempt(TaskAttempt attempt){
            K key = getKeyFromBytes(attempt.getMessage().keyBytes);
            V value = getValueFromBytes(attempt.getMessage().valueBytes);

            try {
                action.apply(key, value);
            } catch (RetryableKStream.RetryableException e) {
                tasksStore.put(attempt.getTimeOfNextAttempt().toInstant().toEpochMilli(), attempt);
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
