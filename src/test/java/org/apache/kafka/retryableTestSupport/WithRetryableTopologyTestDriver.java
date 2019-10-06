package org.apache.kafka.retryableTestSupport;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.retryableTestSupport.extentions.topologyTestDriver.TopologyTestDriverExtension;
import org.apache.kafka.retryableTestSupport.extentions.topologyTestDriver.WithTopologyTestDriver;
import org.apache.kafka.retryableTestSupport.mocks.mockCallbacks.MockCallback;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.internals.RetryableTopologyTestDriver;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Properties;

import static org.apache.kafka.retryableTestSupport.TestTopology.DEFAULT_TEST_INPUT_TOPIC_NAME;

@ExtendWith(TopologyTestDriverExtension.class)
public abstract class WithRetryableTopologyTestDriver implements WithTopologyTestDriver {
    private final Properties topologyProps;
    protected final MockCallback<String, String> action;
    protected TestTopology<String, String> testTopology;
    protected RetryableTopologyTestDriver<String, String> retryableDriver;

    public WithRetryableTopologyTestDriver(MockCallback<String, String> mock, Properties topologyProps){
        this.topologyProps = topologyProps;
        this.action = mock;

        // This Serde differs from the default Serdes defined in the Topology Properties. It is only used when messages
        // are initially piped into the test topology
        final Serde<String> stringSerde = Serdes.String();

        Topology topology = new TopologyFactory<String, String>().build(mock.getCallback(), DEFAULT_TEST_INPUT_TOPIC_NAME,
                                                                        stringSerde, stringSerde);
        this.setTopology(topology, stringSerde);

    }

    @Override
    public TopologyTestDriver getTopologyTestDriver() {
        return this.retryableDriver.getTopologyTestDriver();
    }

    @Override
    public void createTopologyTestDriver(){
        this.retryableDriver = new RetryableTopologyTestDriver<>(testTopology, topologyProps);
    }

    protected void setTopology(Topology topology, Serde<String> serde){
        this.testTopology = new TestTopology<>(topology, DEFAULT_TEST_INPUT_TOPIC_NAME, serde, serde);
    }
}
