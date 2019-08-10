package org.apache.kafka.streams;

import org.apache.kafka.streams.kstream.KStream;

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
public interface RetryableKStream extends KStream {
}
