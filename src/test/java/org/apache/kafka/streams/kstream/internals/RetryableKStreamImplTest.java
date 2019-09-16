package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.retryableTest.WithRetryableTopologyTestDriver;
import org.apache.kafka.retryableTest.extentions.mockCallbacks.MockSuccessfulForeachExtension;
import org.apache.kafka.retryableTest.mockCallbacks.MockSuccessfulForeach;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.processor.StateStore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.retryableTest.TestTopology.DEFAULT_TEST_INPUT_TOPIC_NAME;
import static org.apache.kafka.retryableTest.TestTopology.DEFAULT_TEST_NODE_NAME;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockSuccessfulForeachExtension.class)
class RetryableKStreamImplTest extends WithRetryableTopologyTestDriver {

    RetryableKStreamImplTest(MockSuccessfulForeach<String, String> mockForeach, Properties topologyProps) {
        super(mockForeach, topologyProps);
        setupMultiRetryableForeachNodeTestTopology();
    }

    @Test
    @DisplayName("Should add all of the retryable node to the topology")
    void testAddsRetryableNodeToTopology() {
        assertEquals(3, this.retryableDriver.getTestTopology().getAllRetryProcessors().size());
    }

    @Test
    @DisplayName("Should use a unique state store for each retryable node")
    @SuppressWarnings("unchecked") // I can not figure out why an unchecked conversion is happening with the result of getAllRetryProcessors
    void testUniqueStateStorePerRetryableNode() {
        Collection<TopologyDescription.Processor> processors = retryableDriver.getTestTopology().getAllRetryProcessors().values();
        Set<String> processorStores = processors.stream()
                .flatMap(processor -> processor.stores().stream())
                .collect(Collectors.toSet());
        Set<String> topologyStores = this.retryableDriver.getAllAttemptStores().keySet();

        assertEquals(3, topologyStores.size());
        assertEquals(topologyStores, processorStores);
    }

    @Test
    @DisplayName("It adds the dead letter publishing node as a successor of retryable nodes")
    @SuppressWarnings("unchecked") // I can not figure out why an unchecked conversion is happening with the result of getAllRetryProcessors
    void testDeadLetterNode(){
        Map<String, TopologyDescription.Node> mapOfNodes = retryableDriver.getTestTopology().getAllRetryProcessors();
        mapOfNodes.values().forEach(this::assertHasDeadLetterProcessorSuccessor);
    }

    @Test
    @Disabled
    @DisplayName("Key and Value Serdes can be configured via Kafka Streams conventions")
    void configurableSerdes(){
        /* From AbstractStream:
         *
         * Any classes (KTable, KStream, etc) extending this class should follow the serde specification precedence ordering as:
         *
         * 1) Overridden values via control objects (e.g. Materialized, Serialized, Consumed, etc)
         * 2) Serdes that can be inferred from the operator itself (e.g. groupBy().count(), where value serde can default to `LongSerde`).
         * 3) Serde inherited from parent operator if possible (note if the key / value types have been changed, then the corresponding serde cannot be inherited).
         * 4) Default serde specified in the config.
         */
    }

    private void assertHasDeadLetterProcessorSuccessor(TopologyDescription.Node node){
        Set<TopologyDescription.Node> deadLetterPublisherNodes = node.successors().stream()
                .filter(successor -> successor.name().startsWith(DeadLetterPublisherNode.DEAD_LETTER_PUBLISHER_NODE_PREFIX))
                .collect(Collectors.toSet());

        assertEquals(1, deadLetterPublisherNodes.size(), "Dead Letter Producer Node not found as a successor for Retryable Node");
    }

    private void setupMultiRetryableForeachNodeTestTopology(){
        Serde<String> serde = Serdes.String();
        final StreamsBuilder builder = new StreamsBuilder();
        final Consumed<String, String> consumed = Consumed.with(serde, serde);
        final KStream<String, String> kStream = builder.stream(DEFAULT_TEST_INPUT_TOPIC_NAME, consumed);
        final RetryableKStream<String, String> retriableStream = RetryableKStream.fromKStream(kStream);
        retriableStream.retryableForeach(action.getCallback(), DEFAULT_TEST_NODE_NAME);
        retriableStream.retryableForeach(action.getCallback(), DEFAULT_TEST_NODE_NAME.concat("2"));
        retriableStream.retryableForeach(action.getCallback(), DEFAULT_TEST_NODE_NAME.concat("3"));

        this.setTopology(builder.build(), serde);
    }
}
