package org.apache.kafka.retryableTest;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.retryableTest.extentions.TopologyPropertiesExtension;
import org.apache.kafka.retryableTest.mockCallbacks.MockCallback;
import org.apache.kafka.streams.kstream.internals.RetryableProcessorTestDriver;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Properties;

@ExtendWith(TopologyPropertiesExtension.class)
public class WithRetryableMockProcessorContext {
    protected RetryableProcessorTestDriver<String, String> processorTestDriver;

    public WithRetryableMockProcessorContext(MockCallback<String, String> mockCallback, Properties topologyProps){
        final Serde<String> stringSerde = Serdes.String();
        this.processorTestDriver = new RetryableProcessorTestDriver<>(mockCallback, topologyProps, stringSerde, stringSerde);
    }
}
