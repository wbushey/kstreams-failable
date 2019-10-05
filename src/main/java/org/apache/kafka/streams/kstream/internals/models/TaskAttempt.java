package org.apache.kafka.streams.kstream.internals.models;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * A TaskAttmpt represents a received message that is enqueued to be retried. TaskAttempts include the received message and metadata
 * about the past and current attempts to successfully process the message.
 *
 */
public class TaskAttempt implements Serializable {
    // TODO MAX_ATTEMPTS should be configurable
    public static final Integer MAX_ATTEMPTS = 10;

    // Base number of seconds to wait before reattempting a task. Total time to delay for a given attempt is
    // BASE_RETRY_BACKOFF_SECONDS ^ attemptsCount.
    private static final Integer BASE_RETRY_BACKOFF_SECONDS = 10;

    private final ZonedDateTime timeReceived = now();
    private final String topicOfOrigin;
    private Integer attemptsCount = 1;
    private ZonedDateTime timeOfNextAttempt;
    private final Message message;


    public TaskAttempt(String topicOfOrigin, byte[] keyBytes, byte[] valueBytes) {
        this.topicOfOrigin = topicOfOrigin;
        this.message = new Message(keyBytes, valueBytes);
        this.timeOfNextAttempt = getNewTimeOfNextAttempt(this.attemptsCount);
    }

    public ZonedDateTime getTimeReceived() {
        return timeReceived;
    }

    /**
     * @return The name of the topic that the message associated with the task was received from.
     */
    public String getTopicOfOrigin() {
        return topicOfOrigin;
    }

    /**
     * @return The number of times the task has already been attempted.
     */
    public Integer getAttemptsCount(){
        return attemptsCount;
    }

    /**
     * @return The time that the next attempt of the task should occur at.
     */
    public ZonedDateTime getTimeOfNextAttempt() {
        return timeOfNextAttempt;
    }

    public void setTimeOfNextAttempt(ZonedDateTime timeOfNextAttempt) {
        this.timeOfNextAttempt = timeOfNextAttempt;
    }

    public Message getMessage() {
        return message;
    }

    /**
     * Prepares the TaskAttempt for its next attempt. This should be called after a failed attempt and before writing the TaskAttempt to an attempts store.
     * 
     * Increases attemptCount by 1 and sets timeOfNextAttempt to be a time in the future based on the previous number of attempts.
     *
     */
    public void prepareForNextAttempt(){
        this.attemptsCount++;
        this.timeOfNextAttempt = getNewTimeOfNextAttempt(this.attemptsCount);
    }

    /**
     * @return True if all allowed attempts have occurred, False otherwise.
     */
    public boolean hasExhaustedRetries(){
        return attemptsCount >= MAX_ATTEMPTS;
    }

    public String toString(){
        return "TaskAttempt\n"
            + this.getTimeReceived().toString() + "\n"
            + this.getTopicOfOrigin().toString() + "\n"
            + this.getAttemptsCount().toString() + "\n"
            + this.getTimeOfNextAttempt().toString() + "\n"
            + this.getMessage().toString() + "\n";
    }

    @Override
    public int hashCode(){
        return new HashCodeBuilder()
                .append(attemptsCount)
                .append(timeOfNextAttempt)
                .append(timeReceived)
                .append(topicOfOrigin)
                .append(message)
                .toHashCode();
    }

    @Override
    public boolean equals(Object o){
        if (o == null)
            return false;

        if (o == this)
            return true;


        if (!(o instanceof TaskAttempt))
            return false;

        TaskAttempt other = (TaskAttempt)o;

        return this.hashCode() == other.hashCode();
    }

    public static class Message implements Serializable {
        public final byte[] keyBytes;
        public final byte[] valueBytes;

        private Message(byte[] keyBytes, byte[] valueBytes) {
            this.keyBytes = keyBytes;
            this.valueBytes = valueBytes;
        }

        public String toString(){
            return "TaskAttempt.Message\n"
                + this.keyBytes + "\n"
                + this.valueBytes + "\n";
        }

        @Override
        public int hashCode(){
            return new HashCodeBuilder()
                    .append(keyBytes)
                    .append(valueBytes)
                    .toHashCode();
        }

        @Override
        public boolean equals(Object o){
            if (o == null)
                return false;

            if (o == this)
                return true;

            if (!(o instanceof Message))
                return false;

            Message other = (Message)o;

            return this.hashCode() == other.hashCode();
        }
    }

    private static ZonedDateTime now(){
        return ZonedDateTime.now(ZoneOffset.UTC);
    }

    private static ZonedDateTime getNewTimeOfNextAttempt(Integer attempsCount){
        return now().plus(Duration.ofSeconds((long)Math.pow(BASE_RETRY_BACKOFF_SECONDS, attempsCount)));
    }
}
