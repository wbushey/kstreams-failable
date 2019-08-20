package org.apache.kafka.streams.kstream.internals.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;

import java.io.IOException;
import java.util.Map;

public class TaskAttemptSerde implements Serde<TaskAttempt> {
    private final TaskSerializer serializer = new TaskSerializer();
    private final TaskDeserializer deserializer = new TaskDeserializer();

    @Override
    public Serializer<TaskAttempt> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<TaskAttempt> deserializer() {
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


    private static class TaskSerializer implements Serializer<TaskAttempt>{
        @Override
        public byte[] serialize(String topic, TaskAttempt taskAttempt) {
            byte[] result = null;
            try {
                result = TaskAttempt.serialize(taskAttempt);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return result;
        }
    }

    private static class TaskDeserializer implements Deserializer<TaskAttempt>{
        @Override
        public TaskAttempt deserialize(String topic, byte[] data) {
            TaskAttempt result = null;
            try {
                result = TaskAttempt.deserialize(data);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }

            return result;
        }
    }
}
