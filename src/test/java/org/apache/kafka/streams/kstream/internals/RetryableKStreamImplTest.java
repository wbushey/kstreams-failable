package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.retryableTest.RetryableTestDriverTest;
import org.apache.kafka.retryableTest.extentions.mockCallbacks.MockSuccessfulForeachExtension;
import org.apache.kafka.retryableTest.mockCallbacks.MockSuccessfulForeach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockSuccessfulForeachExtension.class)
class RetryableKStreamImplTest extends RetryableTestDriverTest {

    RetryableKStreamImplTest(MockSuccessfulForeach<String, String> mockForeach, Properties topologyProps) {
        super(mockForeach, topologyProps);
    }

    @Test
    @DisplayName("Should add the retryable node to the topology")
    void addsRetryableNodeToTopology() {
        assertEquals(1, this.retryableDriver.getAllRetryNodes().size());
        assertNotNull(this.retryableDriver.getRetryNode());
    }

    @Test
    @DisplayName("Should use a unique state store for each retryable node")
    void uniqueStateStorePerRetryableNode() {
        assertEquals(1, this.retryableDriver.getAllRetriesStateStores().size());
        assertNotNull(this.retryableDriver.getRetriesStateStore());
    }
}
