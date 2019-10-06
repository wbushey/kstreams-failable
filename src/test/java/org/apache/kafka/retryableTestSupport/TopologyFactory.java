package org.apache.kafka.retryableTestSupport;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.retryableTestSupport.mocks.mockSerdes.MockDefaultSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.kstream.internals.RetryableForeachAction;

import java.util.Properties;

import static org.apache.kafka.retryableTestSupport.TestTopology.DEFAULT_TEST_NODE_NAME;

public class TopologyFactory<K, V> {

    /**
     * @return A new set of topology configuration appropriate for testing
     */
    public static Properties createTopologyProps(){
        return resetTopologyProps(new Properties());
    }

    /**
     * @param topologyProps The topology configuration to reset
     * @return The provided topology configuration, reset with appropriate settings for testing
     */
    public static Properties resetTopologyProps(Properties topologyProps){
        topologyProps.clear();
        topologyProps.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        topologyProps.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        topologyProps.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        topologyProps.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        return topologyProps;
    }


    /**
     * Builds a basic, one node Topology containing a RetryableForeach node that executes the provided action.
     *
     * @param action            Action that should be attempted by the RetryableForeach node
     * @param keySerde          Serde to use for keys
     * @param valueSerde        Serde to use for values
     * @param inputTopicName    Name to use for the input topic of the Topology
     * @return                  A Topology containing a RetryableForeach node
     */
    public Topology build(RetryableForeachAction<K, V> action, String inputTopicName, Serde<K> keySerde, Serde<V> valueSerde){
        final StreamsBuilder builder = new StreamsBuilder();
        final Consumed<K, V> consumed = Consumed.with(keySerde, valueSerde);
        final KStream<K, V> kStream = builder.stream(inputTopicName, consumed);
        final RetryableKStream<K, V> retriableStream = RetryableKStream.fromKStream(kStream);
        retriableStream.retryableForeach(action, DEFAULT_TEST_NODE_NAME);

        return builder.build();
    }


    /**
     * @return Provided Properties with the default Key and Value Serdes set to be MockDefaultSerde
     */
    public static Properties insertMockDefaultSerde(Properties topologyProps){
        topologyProps.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, MockDefaultSerde.class.getName());
        topologyProps.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MockDefaultSerde.class.getName());
        return topologyProps;
    }

}
