package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.retryableTest.WithRetryableTopologyTestDriver;
import org.apache.kafka.retryableTest.extentions.mockCallbacks.MockSuccessfulForeachExtension;
import org.apache.kafka.retryableTest.mockCallbacks.MockSuccessfulForeach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockSuccessfulForeachExtension.class)
class RetryableKStreamImplTest extends WithRetryableTopologyTestDriver {


    RetryableKStreamImplTest(MockSuccessfulForeach<String, String> mockForeach, Properties topologyProps) {
        super(mockForeach, topologyProps);
    }

    @Test
    @DisplayName("Should add the retryable node to the topology")
    void addsRetryableNodeToTopology() {
        // TODO This should test for more than 1 retryable node
        assertEquals(1, this.retryableDriver.getTestTopology().getAllRetryNodes().size());
        assertNotNull(this.retryableDriver.getTestTopology().getRetryNode());
    }

    @Test
    @DisplayName("Should use a unique state store for each retryable node")
    void uniqueStateStorePerRetryableNode() {
        // TODO This should test for more than 1 retryable node
        assertEquals(1, this.retryableDriver.getAllAttemptStateStores().size());
        assertNotNull(this.retryableDriver.getAttemptStateStore());
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

}
