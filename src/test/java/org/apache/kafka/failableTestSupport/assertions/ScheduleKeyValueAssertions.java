package org.apache.kafka.failableTestSupport.assertions;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;

import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScheduleKeyValueAssertions {
    /**
     * Decorates a provided KeyValue<Long, TaskAttempt> with methods that assert expectations.
     * @param attemptKeyValue
     * @return
     */
    public static ScheduleKeyValueAssertions expect(KeyValue<Long, TaskAttempt> attemptKeyValue){
        return new ScheduleKeyValueAssertions(attemptKeyValue);
    }

    public void toBeAttemptForMessage(String key, String value, String topicName, Deserializer<String> keyDeserializer, Deserializer<String> valueDeserializer){
        assertEquals(key, keyDeserializer.deserialize(topicName, attemptKeyValue.value.getMessage().keyBytes));
        assertEquals(value, valueDeserializer.deserialize(topicName, attemptKeyValue.value.getMessage().valueBytes));
    }

    public void toBeScheduledLaterThan(ZonedDateTime time){
        toBeScheduledLaterThan(time.toInstant().toEpochMilli());
    }

    public void toBeScheduledLaterThan(Long time){
        assertTrue(attemptKeyValue.key > time);
    }

    public void toHaveMoreAttemptsThan(Integer count){
        assertTrue(attemptKeyValue.value.getAttemptsCount() > count);
    }

    private final KeyValue<Long, TaskAttempt> attemptKeyValue;

    private ScheduleKeyValueAssertions(KeyValue<Long, TaskAttempt> attemptKeyValue){
        this.attemptKeyValue = attemptKeyValue;
    }
}
