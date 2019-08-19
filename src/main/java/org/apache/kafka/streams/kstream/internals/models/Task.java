package org.apache.kafka.streams.kstream.internals.models;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * A Task represents a received message that is enqueued to be retried. Tasks include the received message and metadata
 * about the past and current attempts to successfully process the message.
 *
 * @param <K> Type of message Keys
 * @param <V> Type of message Values
 */
public class Task <K, V>{
    // Base number of seconds to wait before reattempting a task. Total time to delay for a given attempt is
    // BASE_RETRY_BACKOFF_SECONDS ^ (AttemptNumber -1) .
    private static final Integer BASE_RETRY_BACKOFF_SECONDS = 2;

    // TODO maxAttempts should be configurable
    private final Integer maxAttempts = 10;
    private final LocalDateTime timeReceived = LocalDateTime.now();
    private final String topicOfOrigin;
    private Integer attemptsCount = 1;
    private LocalDateTime timeOfNextAttempt;
    private final Message message;


    public Task(String topicOfOrigin, K key, V value) {
        this.topicOfOrigin = topicOfOrigin;
        this.message = new Message<>(key, value);
        updateTimeOfNextAttempt();
    }

    private void updateTimeOfNextAttempt(){
        this.timeOfNextAttempt = LocalDateTime.now().plus(Duration.ofSeconds(BASE_RETRY_BACKOFF_SECONDS));
    }

    public LocalDateTime getTimeReceived() {
        return timeReceived;
    }

    public String getTopicOfOrigin() {
        return topicOfOrigin;
    }

    public LocalDateTime getTimeOfNextAttempt() {
        return timeOfNextAttempt;
    }

    public void setTimeOfNextAttempt(LocalDateTime timeOfNextAttempt) {
        this.timeOfNextAttempt = timeOfNextAttempt;
    }

    public Message getMessage() {
        return message;
    }

    static class Message<K, V>{
        public final K key;
        public final V value;

        private Message(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }
}
