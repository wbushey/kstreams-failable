package org.apache.kafka.retryableTest;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.retryableTest.extentions.topologyTestDriver.TopologyTestDriverExtension;
import org.apache.kafka.retryableTest.extentions.topologyTestDriver.TopologyTestDriverProvider;
import org.apache.kafka.retryableTest.mockCallbacks.MockCallback;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Properties;

@ExtendWith(TopologyTestDriverExtension.class)
public abstract class RetryableTestDriverTest implements TopologyTestDriverProvider {
    protected final MockCallback<String, String> action;
    protected final RetryableKStreamTestDriver<String, String> retryableDriver;

    public RetryableTestDriverTest(MockCallback<String, String> mock, Properties topologyProps){
        this.action = mock;
        final Serde<String> stringSerde = Serdes.String();
        this.retryableDriver = new RetryableKStreamTestDriver<>(mock.getCallback(),
                stringSerde, stringSerde, topologyProps);
    }

    @Override
    public TopologyTestDriver getTopologyTestDriver() {
        return this.retryableDriver.getTopologyTestDriver();
    }
}
