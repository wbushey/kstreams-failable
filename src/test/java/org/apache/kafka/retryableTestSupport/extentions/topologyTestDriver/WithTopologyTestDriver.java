package org.apache.kafka.retryableTestSupport.extentions.topologyTestDriver;

import org.apache.kafka.streams.TopologyTestDriver;

public interface WithTopologyTestDriver {
    /**
     * @return A TopologyTestDriver
     */
    public TopologyTestDriver getTopologyTestDriver();

    /**
     * Create a TopologyTestDriver within the object. Call {@code getTopologyTestDriver()}
     * to retrieve a created TopologyTestDriver.
     */
    public void createTopologyTestDriver();
}
