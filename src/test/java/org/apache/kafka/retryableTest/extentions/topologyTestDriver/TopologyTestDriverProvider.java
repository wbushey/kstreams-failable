package org.apache.kafka.retryableTest.extentions.topologyTestDriver;

import org.apache.kafka.streams.TopologyTestDriver;

public interface TopologyTestDriverProvider {
    public TopologyTestDriver getTopologyTestDriver();
}
