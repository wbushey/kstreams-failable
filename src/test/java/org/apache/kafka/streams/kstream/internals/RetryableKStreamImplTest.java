package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.retryableTest.Pair;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RetryableKStreamImplTest {
    private final Properties props = new Properties();

    /*
     * Mock ForeachActions and related helpers
     */
    private final List<Pair> receivedRecords = new LinkedList<>();
    private final RetryableForeachAction<String, String> mockForeach = (key, value) -> {
        receivedRecords.add(new Pair<>(key, value));
    };

    private final String TEST_INPUT_TOPIC_NAME = "testTopic";
    private final Consumed<String, String> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());

    @BeforeEach
    void setup(){
        props.clear();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    }

    @Test
    @DisplayName("Should add the retryable node to the topology")
    void addsRetryableNodeToTopology(){
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> kStream = builder.stream(TEST_INPUT_TOPIC_NAME, stringConsumed);
        RetryableKStream<String, String> retryableStream = RetryableKStream.fromKStream(kStream);
        retryableStream.retryableForeach(mockForeach);

        Topology topology = builder.build();

        // Look through the nodes in the topology for one that contains the prefix for a retryableForeach
        boolean found = false;
        Iterator<TopologyDescription.Subtopology> subtopologyIterator = topology.describe().subtopologies().iterator();
        while (!found && subtopologyIterator.hasNext()){
            Iterator<TopologyDescription.Node> nodeIterator = subtopologyIterator.next().nodes().iterator();
            while (!found && nodeIterator.hasNext()){
                found = nodeIterator.next().name().contains("RETRYABLEKSTREAM-RETRYABLE_FOREACH");
            }
        }
        topology.describe().subtopologies().iterator().next().nodes().iterator().next();
        assertTrue(found, "Did not find RetryableForeach node in topology");
    }


    @Test
    @DisplayName("Should use a unique state store for each retryable node")
    void uniqueStateStorePerRetryableNode(){
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> kStream = builder.stream(TEST_INPUT_TOPIC_NAME, stringConsumed);
        RetryableKStream<String, String> retriableStream = RetryableKStream.fromKStream(kStream);
        retriableStream.retryableForeach(mockForeach);

        TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props);
        assertEquals(1, driver.getAllStateStores().size());
        assertTrue(false);
    }
}
