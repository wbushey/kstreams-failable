package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.internals.graph.StreamSinkNode;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.internals.StaticTopicNameExtractor;

public class DeadLetterPublisherNode{
    public static final String DEAD_LETTER_TOPIC_NAME = "dead-letters";
    public static final String DEAD_LETTER_PUBLISHER_NODE_PREFIX = "KSTREAMS-DEAD_LETTER_PUBLISHER-";

    private static final Produced<String, String> dlProduced = Produced.with(Serdes.String(), Serdes.String(), null);
    private static final ProducedInternal<String, String> producedInternal = new ProducedInternal<>(dlProduced);
    private static final TopicNameExtractor<String, String> topicExtractor = new StaticTopicNameExtractor<>(DEAD_LETTER_TOPIC_NAME);

    public static StreamSinkNode<String, String> get(String nodeName){
        return new StreamSinkNode<>(
                nodeName,
                topicExtractor,
                producedInternal
        );
    }
}
