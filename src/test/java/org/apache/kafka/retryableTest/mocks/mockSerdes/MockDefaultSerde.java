package org.apache.kafka.retryableTest.mocks.mockSerdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.retryableTest.mocks.CallTracker;

import java.util.Arrays;

public class MockDefaultSerde implements Serde<String> {
    public static final CallTracker methodCalls = new CallTracker(Arrays.asList("serializer", "deserializer"));
    private Serde<String> stringSerde = Serdes.String();

    @Override
    public Serializer<String> serializer() {
        methodCalls.incrementCallCount("serializer");
        return stringSerde.serializer();
    }

    @Override
    public Deserializer<String> deserializer() {
        methodCalls.incrementCallCount("deserializer");
        return stringSerde.deserializer();
    }
}
