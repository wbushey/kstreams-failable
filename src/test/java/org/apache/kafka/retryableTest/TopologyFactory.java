package org.apache.kafka.retryableTest;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.kstream.internals.RetryableForeachAction;

import static org.apache.kafka.retryableTest.TestTopology.DEFAULT_TEST_NODE_NAME;

public class TopologyFactory<K, V> {


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

}
