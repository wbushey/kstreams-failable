package org.apache.kafka.streams.kstream.internals.serialization.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static org.apache.kafka.streams.kstream.internals.serialization.Serialization.fromByteArray;
import static org.apache.kafka.streams.kstream.internals.serialization.Serialization.toByteArray;

class AbstractSerde<T extends Serializable> implements Serde<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSerde.class);
    private final InnerSerializer<T> serializer;
    private final InnerDeserializer<T> deserializer;

    AbstractSerde(Class<T> type){
         serializer = new InnerSerializer<>();
         deserializer  = new InnerDeserializer<>(type);
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }

    private static class InnerSerializer<T extends Serializable> implements Serializer<T>{
        @Override
        public byte[] serialize(String topic, T object) {
            byte[] result = null;
            try {
                result = toByteArray(object);
            } catch (IOException e) {
                LOG.error("Exception encountered while serializing object for topic " + topic, e);
            }
            return result;
        }
    }

    private static class InnerDeserializer<T extends Serializable> implements  Deserializer<T>{
        private final Class<T> type;

        InnerDeserializer(Class<T> type){
            this.type = type;
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            T result = null;
            try {
                result = fromByteArray(data, type);
            } catch (IOException | ClassNotFoundException e) {
                LOG.error("Exception encountered while deserializing object from topic " + topic, e);
            }

            return result;
        }
    }
}
