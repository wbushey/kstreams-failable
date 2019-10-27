package org.apache.kafka.failableTestSupport;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;

import java.util.AbstractCollection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TestTopology <K, V> {
    public static final String RETRYABLE_FOREACH_PREFIX = "KSTREAM-RETRYABLE_FOREACH-";
    public static final String DEFAULT_TEST_INPUT_TOPIC_NAME = "TestTopic";
    public static final String DEFAULT_TEST_NODE_NAME = "TestNode";

    private final Topology topology;
    private final String inputTopicName;
    private final Serde<K> inputKeySerde;
    private final Serde<V> inputValueSerde;

    public TestTopology(Topology topology, String inputTopicName, Serde<K> inputKeySerde, Serde<V> inputValueSerde){
        this.topology = topology;
        this.inputTopicName = inputTopicName;
        this.inputKeySerde = inputKeySerde;
        this.inputValueSerde = inputValueSerde;
    }

    public Topology getTopology(){
        return topology;
    }

    public String getInputTopicName(){
        return inputTopicName;
    }

    public Serde<K> getInputKeySerde(){
        return this.inputKeySerde;
    }

    public Serde<V> getInputValueSerde(){
        return this.inputValueSerde;
    }

    /**
     * @return Topology Node of the default Retryable node in the test topology
     */
    public TopologyDescription.Node getRetryProcessor(){
        return getRetryProcessor(DEFAULT_TEST_NODE_NAME);
    }

    /**
     * @param name Retryable node to get
     * @return Topology Node of the specified Retryable node in the test topology
     */
    public TopologyDescription.Node getRetryProcessor(String name){
        return getAllRetryProcessors().get(RETRYABLE_FOREACH_PREFIX.concat(name));
    }

    public Map<String, TopologyDescription.Processor> getAllRetryProcessors(){
        return getAllTopologyNodes()
                .stream()
                .filter(node -> node instanceof TopologyDescription.Processor)
                .map(node -> (TopologyDescription.Processor)node)
                .filter(node -> node.name().startsWith(RETRYABLE_FOREACH_PREFIX))
                .collect(Collectors.toMap(TopologyDescription.Node::name, node -> node));
    }


    private Set<TopologyDescription.Node> getAllTopologyNodes(){
        return topology.describe().subtopologies()
                .stream()
                .collect(
                        HashSet::new,
                        (allNodes, subTopology) -> allNodes.addAll(subTopology.nodes()),
                        (AbstractCollection::addAll)
                );
    }
}
