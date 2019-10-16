[![CircleCI](https://circleci.com/gh/wbushey/kstreams-retryable/tree/master.svg?style=svg)](https://circleci.com/gh/wbushey/kstreams-retryable/tree/master)
# kstreams-retryable
Kafka Streams extension with retryable nodes

## How to Use

```java
// Import RetryableKStream and related exception
import org.apache.kafka.streams.kstream.RetryableKStream;
import org.apache.kafka.streams.kstream.RetryableException;

// Define your topology as normal
final StreamsBuilder builder = new StreamsBuilder();
final KStream<String, String> input = builder.stream("inputTopicName");

// Decorate the topology with Retryable methods;
final RetryableKStream<String, String> retryableStream = RetryableKStream.fromKStream(input);

// Use retryableForEach DSL method
retryableStream.retryableForEach((key, value) -> {
  try {
    apiPushThatIntermittentlyFails(value);
  } catch (Exception e) {
    throw new RetryableException(e);
  }
});
```

## Retryable Behavior

The DSL methods added by this extension handle failure in the blocks provided to them as follows:
 * If the block throws a `FailableException`, the method will not attempt to retry the block.
   A message will be published to a Dead Letter Topic representing the key and value that the block
   attempted to process, as well as data about the number of attempts.
 * If the block throws a `RetryableException`, the method will schedule another attempt to execute
   the block with the same key and value. This scheduled retry will be non-blocking; the node will
   continue to consume messages before and after any scheduled retries. Each successive retry
   will be scheduled with a delay approximately twice as long as the previous delay. If 10 retries
   are attempted that cause RetryableException to be thrown, the provided key and value will be
   considered not retryable, will be published to a Dead Letter Topic as if a FailableException had
   occurred, and will not be scheduled for a retry.

## DSL Methods Added

### retryableForEach

A retryable version of KStream's `foreach` method.

## Development Setup

```shell script
# Install required version of Java
asdf install

# Get Java dependencies
./gradlew dependencies
```

## Run Unit Tests

```shell script
./gradlew test
```
