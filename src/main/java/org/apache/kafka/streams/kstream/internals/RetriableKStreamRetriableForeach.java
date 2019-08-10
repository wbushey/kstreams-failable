package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.RetriableKStream;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class RetriableKStreamRetriableForeach<K, V> implements ProcessorSupplier<K, V> {

    private final RetriableForeachAction<? super K, ? super V> action;
    private final String retriesStoreName;

    RetriableKStreamRetriableForeach(String retriesStoreName, final RetriableForeachAction<? super K, ? super V> action){
        this.retriesStoreName = retriesStoreName;
        this.action = action;
    }

    @Override
    public Processor<K, V> get() { return new RetryableKStreamRetryableForeachProcessor(); }

    private class RetryableKStreamRetryableForeachProcessor extends AbstractProcessor<K, V> {
        private ProcessorContext context;
        private KeyValueStore<K, V> retriesStore;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context){
            this.context = context;

            this.retriesStore = (KeyValueStore) context.getStateStore(retriesStoreName);
        }

        @Override
        public void process(final K key, final V value){
            try {
                action.apply(key, value);
            } catch (RetriableKStream.RetriableException e) {
                retriesStore.put(key, value);
            } catch (RetriableKStream.FailableException e) {
                e.printStackTrace();
            }
        }
    }
}
