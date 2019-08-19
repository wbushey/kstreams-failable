package org.apache.kafka.streams.kstream.internals.models;

import java.io.*;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * A Task represents a received message that is enqueued to be retried. Tasks include the received message and metadata
 * about the past and current attempts to successfully process the message.
 *
 */
public class Task implements Serializable {
    // Base number of seconds to wait before reattempting a task. Total time to delay for a given attempt is
    // BASE_RETRY_BACKOFF_SECONDS ^ (AttemptNumber -1) .
    private static final Integer BASE_RETRY_BACKOFF_SECONDS = 2;

    public static byte[] serialize(Task task) throws IOException {
        return toByteArray(task);
    }

    public static Task deserialize(byte[] data) throws IOException, ClassNotFoundException {
        return fromByteArray(data);
    }



    // TODO maxAttempts should be configurable
    private final Integer maxAttempts = 10;
    private final ZonedDateTime timeReceived = now();
    private final String topicOfOrigin;
    private Integer attemptsCount = 1;
    private ZonedDateTime timeOfNextAttempt;
    private final Message message;


    public Task(String topicOfOrigin, byte[] keyBytes, byte[] valueBytes) {
        this.topicOfOrigin = topicOfOrigin;
        this.message = new Message(keyBytes, valueBytes);
        this.timeOfNextAttempt = getNewTimeOfNextAttempt();
    }

    public ZonedDateTime getTimeReceived() {
        return timeReceived;
    }

    public String getTopicOfOrigin() {
        return topicOfOrigin;
    }

    public ZonedDateTime getTimeOfNextAttempt() {
        return timeOfNextAttempt;
    }

    public void setTimeOfNextAttempt(ZonedDateTime timeOfNextAttempt) {
        this.timeOfNextAttempt = timeOfNextAttempt;
    }

    public Message getMessage() {
        return message;
    }

    public static class Message implements Serializable {
        public final byte[] keyBytes;
        public final byte[] valueBytes;

        private Message(byte[] keyBytes, byte[] valueBytes) {
            this.keyBytes = keyBytes;
            this.valueBytes = valueBytes;
        }
    }

    private static ZonedDateTime now(){
        return ZonedDateTime.now(ZoneOffset.UTC);
    }

    private static ZonedDateTime getNewTimeOfNextAttempt(){
        return now().plus(Duration.ofSeconds(BASE_RETRY_BACKOFF_SECONDS));
    }

    private static byte[] toByteArray(Task task) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(task);
        byte[] result = bos.toByteArray();
        oos.close();
        bos.close();
        return result;
    }

    private static Task fromByteArray(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream boi = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(boi);
        Task result = (Task) ois.readObject();
        ois.close();
        boi.close();
        return result;
    }

}
