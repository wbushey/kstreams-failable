package org.apache.kafka.streams.kstream;

/**
 * Extension of KStreams that adds retryable processing actions.
 *
 * Usage:
 * <pre>{@code
 *      final StreamsBuilder builder = new StreamsBuilder();
 *      final KStream<String, String> stream = builder.stream("inputTopic");
 *      final RetryableKStream<String, String> retryableStream = RetryableKStream.from(stream);
 *
 *      retryableStream.retryableForeach((key, record) -> //Your logic that might fail sometimes that you want to retry);
 * }</pre>
 *
 */
public interface RetriableKStream extends KStream {

    /**
     * An exception representing an error that is likely to automatically heal. Thus, the action that led to this
     * exception should be retired.
     */
    class RetriableException extends Exception {
        public RetriableException(String message) { super(message); }
        public RetriableException(String message, Throwable e) { super(message, e);}
    }

    /**
     * A exception representing an error that is not likely to require manual action to address. Thus, the action
     * that led to this exception should *not* be retried.
     */
    class FailableException extends Exception {
        public FailableException(String message) { super(message); }
        public FailableException(String message, Throwable e) { super(message, e); }
    }
}
