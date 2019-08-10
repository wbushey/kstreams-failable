package org.apache.kafka.streams.internals;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class RetryableKStreamRetryableForeach<K, V> implements ProcessorSupplier<K, V> {

    private final ForeachAction<? super K, ? super V> action;

    RetryableKStreamRetryableForeach(final ForeachAction<? super K, ? super V> action){
        this.action = action;
    }

    @Override
    public Processor<K, V> get() { return new RetryableKStreamRetryableForeachProcessor(); }

    private class RetryableKStreamRetryableForeachProcessor extends AbstractProcessor<K, V> {
        @Override
        public void process(final K key, final V value){
            action.apply(key, value);
        }
    }
}
