package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.kstream.internals.RetriableForeachAction;
import org.apache.kafka.streams.kstream.internals.RetriableKStreamImpl;

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
 * @see KStream
 */
@InterfaceStability.Evolving
public interface RetriableKStream<K, V> extends KStream<K, V> {

    /**
     * Decorates a provided {@KStream} with retriable methods.
     *
     * @param original
     * @return Decorated stream with retriable methods.
     */
    static RetriableKStream fromKStream(KStream original){
        // Assuming KStream is a KStreamImpl :(
        // TODO stop doing that
        return new RetriableKStreamImpl((KStreamImpl)original);
    }

    /**
     * Perform an action on each record of {@code RetriableKStream}. If execution of that action throws a
     * RetriableException, than the action will be retried at a later time, and will not block processing of other
     * messages. If execution of that action throws a FailableException, or if the maximum number of retries is reached,
     * then the message and associated data about it will be written to a dead-letter topic.
     *
     * The record-by-record operation performed by the provided action is stateless. However, a state store is created
     * to store information about actions to retry in the future. The state store's name will be derived from a name
     * generated for the processing node. <strong>Passing a specified {@code Named} via
     * {@code #retriableForeach(RetriableForeachAction, Named)} is highly recommended to ensure a consistent state
     * store name for the processing node across topology changes.</strong>
     *
     * Note that this is a terminal operation that returns void.
     *
     * @param action an action to perform on each record
     * @see #retriableForeach(RetriableForeachAction, Named)
     * @see KStream#foreach(ForeachAction)
     */
    void retriableForeach(final RetriableForeachAction<? super K, ? super V> action);

    /**
     * Perform an action on each record of {@code RetriableKStream}. If execution of that action throws a
     * RetriableException, than the action will be retried at a later time, and will not block processing of other
     * messages. If execution of that action throws a FailableException, or if the maximum number of retries is reached,
     * then the message and associated data about it will be written to a dead-letter topic.
     *
     * The record-by-record operation performed by the provided action is stateless. However, a state store is created
     * to store information about actions to retry in the future. The state store's name will be derived from the
     * provided {@code Named}.
     *
     * Note that this is a terminal operation that returns void.
     *
     * @param action an action to perform on each record
     * @param named  a {@link Named} config used to name the processor in the topology
     * @see KStream#foreach(ForeachAction)
     */
    void retriableForeach(final RetriableForeachAction<? super K, ? super V> action, final Named named);


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
