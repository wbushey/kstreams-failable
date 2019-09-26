package org.apache.kafka.streams.kstream.internals.serialization.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static org.apache.kafka.streams.kstream.internals.serialization.Serialization.fromByteArray;
import static org.apache.kafka.streams.kstream.internals.serialization.Serialization.toByteArray;

class AbstractSerde<T extends Serializable> implements Serde<T> {
    private final InnerSerializer<T> serializer;
    private final InnerDeserializer<T> deserializer;

    AbstractSerde(Class<T> type){
         serializer = new InnerSerializer<>();
         deserializer  = new InnerDeserializer<>(type);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
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
                e.printStackTrace();
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
                e.printStackTrace();
            }

            return result;
        }
    }
}
