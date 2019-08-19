package org.apache.kafka.streams.kstream.internals.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.internals.models.Task;

import java.io.IOException;
import java.util.Map;

public class TaskSerde implements Serde<Task> {
    private final TaskSerializer serializer = new TaskSerializer();
    private final TaskDeserializer deserializer = new TaskDeserializer();

    @Override
    public Serializer<Task> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Task> deserializer() {
        return deserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey){
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close(){
        serializer.close();
        deserializer.close();
    }


    private static class TaskSerializer implements Serializer<Task>{
        @Override
        public byte[] serialize(String topic, Task task) {
            byte[] result = null;
            try {
                result = Task.serialize(task);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return result;
        }
    }

    private static class TaskDeserializer implements Deserializer<Task>{
        @Override
        public Task deserialize(String topic, byte[] data) {
            Task result = null;
            try {
                result = Task.deserialize(data);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }

            return result;
        }
    }
}
