package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.kstream.internals.FailableForeachAction;
import org.apache.kafka.streams.kstream.internals.FailableKStreamImpl;

/**
 * Extension of KStreams that adds error handling processing actions.
 *
 * Usage:
 * <pre>{@code
 *      final StreamsBuilder builder = new StreamsBuilder();
 *      final KStream<String, String> stream = builder.stream("inputTopic");
 *      final FailableKStream<String, String> failableStream = FailableKStream.from(stream);
 *
 *      failableStream.retryableForeach((key, record) -> //Your logic that might fail sometimes that you want to handle);
 * }</pre>
 *
 * @see KStream
 */
@InterfaceStability.Evolving
public interface FailableKStream<K, V> extends KStream<K, V> {

    /**
     * Decorates a provided {@KStream} with failure handling methods.
     *
     * @param original
     * @return Decorated stream with failure handling methods.
     */
    static <K, V> FailableKStream<K, V> fromKStream(KStream<K, V> original){
        // Assuming KStream is a KStreamImpl :(
        // TODO stop doing that
        return new FailableKStreamImpl<>((KStreamImpl<K, V>) original);
    }

    /**
     * Perform an action on each record of {@code FailableKStream}. If execution of that action throws a
     * RetryableException, than the action will be retried at a later time, and will not block processing of other
     * messages. If execution of that action throws a FailableException, or if the maximum number of retries is reached,
     * then the message and associated data about it will be written to a dead-letter topic.
     *
     * The record-by-record operation performed by the provided action is stateless. However, a state store is created
     * to store information about actions to retry in the future. The state store's name will be derived from a name
     * generated for the processing node. <strong>Passing a specified name via
     * {@code #retryableForeach(FailableForeachAction, String)} is highly recommended to ensure a consistent state
     * store name for the processing node across topology changes.</strong>
     *
     * Note that this is a terminal operation that returns void.
     *
     * @param action an action to perform on each record
     * @see #retryableForeach(FailableForeachAction, String)
     * @see KStream#foreach(ForeachAction)
     */
    void retryableForeach(final FailableForeachAction<? super K, ? super V> action);

    /**
     * Perform an action on each record of {@code FailableKStream}. If execution of that action throws a
     * RetryableException, than the action will be retried at a later time, and will not block processing of other
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
     * @param name   used to name the processor in the topology
     * @see KStream#foreach(ForeachAction)
     */
    void retryableForeach(final FailableForeachAction<? super K, ? super V> action, final String name);


    /**
     * An exception representing an error that is likely to automatically heal. Thus, the action that led to this
     * exception should be retired.
     */
    class RetryableException extends RuntimeException {
        public RetryableException(String message) { this(message, null); }
        public RetryableException(String message, Throwable e) { super(message, e);}
    }

    /**
     * A exception representing an error that is not likely to require manual action to address. Thus, the action
     * that led to this exception should *not* be retried.
     */
    class FailableException extends RuntimeException {
        public FailableException(String message) { this(message, null); }
        public FailableException(String message, Throwable e) { super(message, e); }
    }

    /**
     * An exception representing too many attempts being made to successfully execute a task. Subclass of
     * FailableException, thus, the action should not be retried again.
     */
    class RetriesExhaustedException extends FailableException {
        public RetriesExhaustedException(String message) { this(message, null);}
        public RetriesExhaustedException(String message, Throwable e) { super(message, e);}
    }
}
