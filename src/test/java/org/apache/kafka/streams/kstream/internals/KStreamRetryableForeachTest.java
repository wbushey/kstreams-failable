package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.retryableTest.mockCallbacks.MockForeach;
import org.apache.kafka.retryableTest.mockCallbacks.MockRetryableExceptionForeach;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.retryableTest.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;


@ExtendWith(org.apache.kafka.retryableTest.extentions.MockForeach.class)
@ExtendWith(org.apache.kafka.retryableTest.extentions.MockRetryableExceptionForeach.class)
class KStreamRetryableForeachTest {
    /*
     * Mock ForeachActions and related helpers
     */
    private final MockForeach<String, String> mockForeach;
    private final MockRetryableExceptionForeach<String, String> mockRetryableExceptionForeach;

    /*
     * StateStore and ProcessorContext used for these tests
     */
    private final String RETRIES_STORE_NAME = "retiresStore";
    private final StoreBuilder<KeyValueStore<String, String>> retriesStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(RETRIES_STORE_NAME),
            Serdes.String(), Serdes.String()
    );
    private final KeyValueStore<String, String> retriesStore = retriesStoreBuilder.build();
    private final ProcessorContext mockContext = mock(ProcessorContext.class);

    // Test constructor
    KStreamRetryableForeachTest(MockForeach<String, String> mockForeach, MockRetryableExceptionForeach<String, String> mockRetryableExceptionForeach){
        this.mockForeach = mockForeach;
        this.mockRetryableExceptionForeach = mockRetryableExceptionForeach;
    }

    @BeforeEach
    void setup(){
        // Clear the testing retries store
        retriesStore.all().forEachRemaining((keyValue) -> retriesStore.delete(keyValue.key));

        // Clear anything that happened to the mockContext
        reset(mockContext);
        when(mockContext.getStateStore(RETRIES_STORE_NAME)).thenReturn(retriesStore);
    }
    
    @Test
    @DisplayName("It immediately attempts to execute the provided block")
    void testImmediateExecution(){
        final Processor<String, String> subject = new KStreamRetryableForeach<>(RETRIES_STORE_NAME, mockForeach.getCallback()).get();
        subject.process("key", "value");
        assertEquals(Collections.singletonList(new Pair<>("key", "value")), mockForeach.getReceivedRecords());
    }

    @Test
    @DisplayName("It schedules a retry via the retries state store if a RetryableException is thrown by the block")
    void testSchedulingRetry(){
        final Processor<String, String> subject = new KStreamRetryableForeach<>(RETRIES_STORE_NAME, mockRetryableExceptionForeach.getCallback()).get();
        subject.init(mockContext);
        subject.process("key", "value");
        List<KeyValue<String, String>> scheduledJobs = new LinkedList<>();
        retriesStore.all().forEachRemaining(scheduledJobs::add);
        assertEquals(1, scheduledJobs.size());
        assertEquals("value", scheduledJobs.get(0).value);
    }

    @Disabled
    @Test
    @DisplayName("It deletes a retry from the retries state store once it is been executed successfully")
    void testRetryDeletionOnSuccess(){}

    @Disabled
    @Test
    @DisplayName("It deletes a retry from the retries state store once it is been executed and thrown a RetryableException")
    void testRetryDeletionOnRetryableException(){}

    @Disabled
    @Test
    @DisplayName("It deletes a retry from the retries state store once it is been executed and thrown a FailableException")
    void testRetryDeletionOnFailableException(){}

    @Disabled
    @Test
    @DisplayName("It schedules retries via an exponential backoff based on number of retries already attempted")
    void testExponentialBackoffScheduling(){}

    @Disabled
    @Test
    @DisplayName("It schedules another retry via the retries state store if a retry is executed and throws a RetryableException")
    void testRetryRetryOnRetryableException(){}

    @Disabled
    @Test
    @DisplayName("It schedules retry punctuation at appropriate interval")
    void testPunctuateScheduling(){}

    @Disabled
    @Test
    @DisplayName("It executes scheduled retries on punctuate")
    void testPunctuateExecution(){}

    @Disabled
    @Test
    @DisplayName("On punctuate, it executes scheduled retries recovered from application crash")
    void testRecoveringScheduledRetries(){}

    @Disabled
    @Test
    @DisplayName("On punctuate, it immediately executes scheduled retries recovered from application crash that should have already been executed")
    void testImmediateExecutionOfRecoveredScheduledRetries(){}

    @Disabled
    @Test
    @DisplayName("It publishes a message to a dead letter topic when a FailableException is thrown by the block")
    void testPublishingFailableException(){}

    @Disabled
    @Test
    @DisplayName("It does not schedule a retry via the retires state store when a FailableException is thrown by the block")
    void testNoRetryScheduledOnFailableException(){}

    @Disabled
    @Test
    @DisplayName("It does not schedule a retry via the retires state store when no exception is thrown by the block")
    void testNoRetryScheduledOnSuccess(){}

    @Disabled
    @Test
    @DisplayName("It treats a job that has exhausted it's retries as having thrown a FailableException")
    void testRetryExhaustionException(){}

    @Disabled
    @Test
    @DisplayName("It closes the scheduled retries state store when closed")
    void testCloseStateStoreOnClose(){}

}

