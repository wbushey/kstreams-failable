package org.apache.kafka.streams.kstream.internals.models;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TaskAttemptTest {
    private static final String DEAFULT_TEST_TOPIC_NAME = "TestTopic";
    private ZonedDateTime now;
    private TaskAttempt subject;

    @BeforeEach
    void setUp(){
        final String topicName = DEAFULT_TEST_TOPIC_NAME;
        final Serde<String> stringSerde = Serdes.String();
        now = ZonedDateTime.now(ZoneOffset.UTC);

        subject = new TaskAttempt(
                topicName,
                stringSerde.serializer().serialize(topicName, "key"),
                stringSerde.serializer().serialize(topicName, "value")
                );
    }

    @Test
    @DisplayName("It advances attempt count when preparing for next attempt.")
    void prepareForNextAttemptAdvancesAttemptsCount(){
        for (int i = 1; i <= 10; i++){
            assertEquals(i, subject.getAttemptsCount());
            subject.prepareForNextAttempt();
        }
    }

    @Test
    @DisplayName("It bases advancement of timeOfNextAttempt on 10 second delays when preparing for next attempt.")
    void prepareForNextAttemptAdvancesTimeOfNextAttemptBasedOnTenSecondDelay(){

        final Long nowMs = now.toInstant().toEpochMilli();
        final Long currentAttemptMs = subject.getTimeOfNextAttempt().toInstant().toEpochMilli();

        final Long expectedDelay = Duration.ofSeconds(10).toMillis();
        final Long actualDelay = currentAttemptMs - nowMs;
        assertTrue(expectedDelay <= actualDelay, "Expected actualDelay of " + actualDelay + " to be at least " + expectedDelay);
    }

    @Test
    @DisplayName("It advances timeOfNextAttempt using an exponential backoff based on attemptsCount when preparing for next attempt.")
    void prepareForNextAttemptAdvancesTimeOfNextAttemptExponentally(){
        final List<ZonedDateTime> timesOfNextAttempt = new LinkedList<>();
        for (int i = 1; i <= 10; i++){
            timesOfNextAttempt.add(subject.getTimeOfNextAttempt());
            subject.prepareForNextAttempt();
        }

        for (int i = 2; i <= 9; i++){
            final Long previousPreviousAttemptMs = timesOfNextAttempt.get(i-2).toInstant().toEpochMilli();
            final Long previousAttemptMs = timesOfNextAttempt.get(i-1).toInstant().toEpochMilli();
            final Long currentAttemptMs = timesOfNextAttempt.get(i).toInstant().toEpochMilli();

            final Long previousDelay = previousAttemptMs - previousPreviousAttemptMs;
            final Long currentDelay = currentAttemptMs - previousAttemptMs;
            final Long expectedDelay = (previousDelay * 2);

            assertTrue(expectedDelay <= currentDelay, "Expected " + currentDelay + " to be at least " + expectedDelay );
        }
    }

    @Test
    @DisplayName("hasExhaustedRetries returns true when the maximum number of attempts have occurred.")
    void hasExhaustedRetriesTest(){
        while(subject.getAttemptsCount() < TaskAttempt.MAX_ATTEMPTS){
            assertFalse(subject.hasExhaustedRetries());
            subject.prepareForNextAttempt();
        }

        // The maximum number of attempts have been attempted.
        assertTrue(subject.getAttemptsCount() == TaskAttempt.MAX_ATTEMPTS);
        assertTrue(subject.hasExhaustedRetries());
    }
}
