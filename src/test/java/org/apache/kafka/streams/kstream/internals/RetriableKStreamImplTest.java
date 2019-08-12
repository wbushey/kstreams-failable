package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.RetriableKStream;
import org.apache.kafka.test.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class RetriableKStreamImplTest {
    private final Properties props = new Properties();

    /*
     * Mock ForeachActions and related helpers
     */
    private final List<Pair> receivedRecords = new LinkedList<>();
    private final RetriableForeachAction<String, String> mockForeach = (key, value) -> {
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
    }

    @Disabled
    @Test
    @DisplayName("Should add the retryable node to the topology")
    void addsRetryableNodeToTopology(){
        final StreamsBuilder builder = new StreamsBuilder();
        KStream kStream = builder.stream(TEST_INPUT_TOPIC_NAME, stringConsumed);
        RetriableKStream retriableStream = RetriableKStream.fromKStream(kStream);
        retriableStream.retriableForeach(mockForeach);

        builder.build();
    }


    @Disabled
    @Test
    @DisplayName("Should use a unique state store for each retryable node")
    void uniqueStateStorePerRetryableNode(){}

}
