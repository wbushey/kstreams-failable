package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.retryableTest.extentions.TopologyTestDriverExtention;
import org.apache.kafka.retryableTest.mockCallbacks.MockForeach;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Iterator;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(org.apache.kafka.retryableTest.extentions.MockForeach.class)
class RetryableKStreamImplTest {
    /*
     * Mock ForeachActions and related helpers
     */
    private final MockForeach<String, String> mockForeach;

    private final String TEST_INPUT_TOPIC_NAME = "testTopic";
    private final Consumed<String, String> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());

    RetryableKStreamImplTest(MockForeach<String, String> mockForeach){
        this.mockForeach = mockForeach;
    }

    @Test
    @DisplayName("Should add the retryable node to the topology")
    void addsRetryableNodeToTopology(){
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> kStream = builder.stream(TEST_INPUT_TOPIC_NAME, stringConsumed);
        RetryableKStream<String, String> retryableStream = RetryableKStream.fromKStream(kStream);
        retryableStream.retryableForeach(mockForeach.getCallback());

        Topology topology = builder.build();

        // Look through the nodes in the topology for one that contains the prefix for a retryableForeach
        boolean found = false;
        Iterator<TopologyDescription.Subtopology> subtopologyIterator = topology.describe().subtopologies().iterator();
        while (!found && subtopologyIterator.hasNext()){
            Iterator<TopologyDescription.Node> nodeIterator = subtopologyIterator.next().nodes().iterator();
            while (!found && nodeIterator.hasNext()){
                found = nodeIterator.next().name().startsWith("RETRYABLEKSTREAM-RETRYABLE_FOREACH-");
            }
        }
        topology.describe().subtopologies().iterator().next().nodes().iterator().next();
        assertTrue(found, "Did not find RetryableForeach node in topology");
    }

    @Nested
    @ExtendWith(TopologyTestDriverExtention.class)
    class WithRetryableForEachTopology{
        private final TopologyTestDriver driver;

        WithRetryableForEachTopology(Properties topologyProps){
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> kStream = builder.stream(TEST_INPUT_TOPIC_NAME, stringConsumed);
            final RetryableKStream<String, String> retriableStream = RetryableKStream.fromKStream(kStream);
            retriableStream.retryableForeach(mockForeach.getCallback());

            this.driver = new TopologyTestDriver(builder.build(), topologyProps);
        }

        @Test
        @DisplayName("Should use a unique state store for each retryable node")
        void uniqueStateStorePerRetryableNode(){
            assertEquals(1, this.driver.getAllStateStores().size());

            boolean found = false;
            Iterator<String> keyIterator = this.driver.getAllStateStores().keySet().iterator();
            while (!found && keyIterator.hasNext()){
                found = keyIterator.next().endsWith("-RETRIES_STORE");
            }
            assertTrue(found, "Did not find Retries Store in the topology's state stores");
        }
    }
}
