package org.apache.kafka.streams.internals;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.processor.Processor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;


class RetryableKStreamRetryableForeachTest {
    private final List<Pair> receivedRecords = new LinkedList<Pair>();
    private final ForeachAction<String, String> noopForEach = (key, value) -> {
        receivedRecords.add(new Pair<>(key, value));
    };

    @BeforeEach
    void setup(){
        // Reset records received by mock ForeachActions for consistent expectations
        receivedRecords.clear();
    }
    
    @Test
    @DisplayName("It immediately attempts to execute the provided block")
    void testImmediateExecution(){
        final Processor<String, String> subject = new RetryableKStreamRetryableForeach<String, String>(noopForEach).get();
        subject.process("key", "value");
        assertEquals(Arrays.asList(new Pair<>("key", "value")), receivedRecords);
    }

    @Disabled
    @Test
    @DisplayName("It schedules a retry via the retries state store if a RetryableException is thrown by the block")
    void testSchedulingRetry(){}

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

class Pair<Key, Value> {
  public final Key key;
  public final Value value;
  public Pair(Key key, Value value) {
      this.key = key;
      this.value = value;
  }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return this.hashCode() == pair.hashCode();
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}
