package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.internals.serdes.TaskAttemptSerde;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.*;

class TestAttemptsDAOTest {
    private static final String DEAFULT_TEST_ATTEMPTS_STORE_NAME = "testAttemptsStore";
    private static final String DEAFULT_TEST_TOPIC_NAME = "testTopic";
    private KeyValueStore<Long, TaskAttempt> attemptsStore;

    @BeforeEach
    void setUp(){
        this.attemptsStore = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(DEAFULT_TEST_ATTEMPTS_STORE_NAME), Serdes.Long(), new TaskAttemptSerde())
                .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                .build();

    }

    @Test
    void canScheduleATask(){
        TaskAttempt attempt = createTestTaskAttempt("key", "value");
        assertTrue(false);
    }

    private TaskAttempt createTestTaskAttempt(String key, String value){
        final String topicName = DEAFULT_TEST_TOPIC_NAME;
        final Serde<String> stringSerde = Serdes.String();
        return new TaskAttempt(
                topicName,
                stringSerde.serializer().serialize(topicName, key),
                stringSerde.serializer().serialize(topicName, value)
        );
    }

}
