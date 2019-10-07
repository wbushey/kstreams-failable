package org.apache.kafka.streams.kstream.internals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.kstream.internals.TaskAttemptsStore.TaskAttemptsDAO;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;
import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class KStreamRetryableForeach<K, V> implements ProcessorSupplier<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamRetryableForeach.class);

    private static final Long ATTEMPTS_PUNCTUATE_INTERVAL_MS = 500L;
    private final RetryableForeachAction<? super K, ? super V> action;
    private final String tasksStoreName;
    private final String deadLetterNodeName;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KStreamRetryableForeach(String tasksStoreName, String deadLetterNodeName, final RetryableForeachAction<? super K, ? super V> action){
        this.tasksStoreName = tasksStoreName;
        this.deadLetterNodeName = deadLetterNodeName;
        this.action = action;
    }

    @Override
    public Processor<K, V> get() { return new RetryableKStreamRetryableForeachProcessor(); }

    private class RetryableKStreamRetryableForeachProcessor extends AbstractProcessor<K, V> {
        private ProcessorContext context;
        private TaskAttemptsDAO taskAttemptsDAO;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context){
            this.context = context;


            final KeyValueStore<Long, TaskAttemptsCollection> taskAttemptsStore = (KeyValueStore) context.getStateStore(tasksStoreName);
            this.taskAttemptsDAO = new TaskAttemptsDAO(taskAttemptsStore);

            this.context.schedule(Duration.ofMillis(ATTEMPTS_PUNCTUATE_INTERVAL_MS),
                    PunctuationType.WALL_CLOCK_TIME,
                    this::performAttemptsScheduledFor);
        }

        @Override
        public void process(final K key, final V value){
            TaskAttempt attempt = new TaskAttempt(context.topic(), getBytesOfKey(key), getBytesOfValue(value));
            performAttempt(attempt);
        }

        private void performAttemptsScheduledFor(Long punctuateTimestamp){
            Iterator<KeyValue<Long, TaskAttempt>> scheduledTasks = taskAttemptsDAO.getAllTaskAttemptsScheduledBefore(punctuateTimestamp);
            scheduledTasks.forEachRemaining(scheduledTask -> {
                taskAttemptsDAO.unschedule(scheduledTask.value);
                performAttempt(scheduledTask.value);
            });
        }

        private void performAttempt(TaskAttempt attempt){
            K key = getKeyFromBytes(attempt.getMessage().keyBytes);
            V value = getValueFromBytes(attempt.getMessage().valueBytes);

            try {
                try{
                    action.apply(key, value);
                } catch (RetryableKStream.RetryableException e) {
                    logRetryableException(attempt, key, e);
                    attempt.prepareForNextAttempt();
                    taskAttemptsDAO.schedule(attempt);
                }
            } catch (RetryableKStream.FailableException e) {
                logFailableException(attempt, key, e);
                context.forward(getDLTKey(attempt), jsonify(attempt), To.child(deadLetterNodeName));
            }
        }

        private String getDLTKey(TaskAttempt attempt){
           return attempt.getTopicOfOrigin().concat(".").concat(attempt.getTimeReceived().toString());
        }

        private String jsonify(TaskAttempt attempt){
            String json = "";

            Map<String, String> jsonMap = new HashMap<String, String>(){{
                put("topicOfOrigin", attempt.getTopicOfOrigin());
                put("timeReceived", attempt.getTimeReceived().toString());
                put("attempts", attempt.getAttemptsCount().toString());
            }};

            try {
                Map<String, Object> messageMap = new HashMap<String, Object>(){{
                    put("key", getKeyFromBytes(attempt.getMessage().keyBytes));
                    put("value", getValueFromBytes(attempt.getMessage().valueBytes));
                }};
                jsonMap.put("message", objectMapper.writeValueAsString(messageMap));
                json = objectMapper.writeValueAsString(jsonMap);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return json;
        }

        @SuppressWarnings("unchecked")
        private byte[] getBytesOfKey(K key){
            final String topic = context.topic();
            Serde keySerde = context.keySerde();
            return keySerde.serializer().serialize(topic, key);
        }

        @SuppressWarnings("unchecked")
        private K getKeyFromBytes(byte[] keyBytes){
            final String topic = context.topic();
            Serde keySerde = context.keySerde();
            return (K)keySerde.deserializer().deserialize(topic, keyBytes);
        }

        @SuppressWarnings("unchecked")
        private byte[] getBytesOfValue(V value){
            final String topic = context.topic();
            Serde valueSerde = context.valueSerde();
            return valueSerde.serializer().serialize(topic, value);
        }

        @SuppressWarnings("unchecked")
        private V getValueFromBytes(byte[] valueBytes){
            final String topic = context.topic();
            Serde valueSerde = context.valueSerde();
            return (V)valueSerde.deserializer().deserialize(topic, valueBytes);
        }

        private void logRetryableException(TaskAttempt attempt, K key, RetryableKStream.RetryableException e){
            LOG.warn(
                "A Retryable Error has occurred while processing the following message"
                + "\n\tAttempt Number:\t" + attempt.getAttemptsCount()
                + commonExceptionLogLines(attempt, key, e)
            );
        }

        private void logFailableException(TaskAttempt attempt, K key, RetryableKStream.FailableException e){
            LOG.error(
                "A Non-Retryable Error has occurred while processing the following message"
                + commonExceptionLogLines(attempt, key, e)
            );
        }

        private String commonExceptionLogLines(TaskAttempt attempt, K key, Exception e){
            return  "\n\tTopic:\t\t" + attempt.getTopicOfOrigin()
                    + "\n\tKey:\t\t" + key.toString()
                    + "\n\tError:\t\t" + e.toString();
        }
    }
}
