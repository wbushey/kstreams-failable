package org.apache.kafka.streams.kstream.internals.serialization.serdes;

import org.apache.kafka.streams.kstream.internals.models.TaskAttemptsCollection;


public class TaskAttemptsCollectionSerde extends AbstractSerde<TaskAttemptsCollection> {
    public TaskAttemptsCollectionSerde(){
        super(TaskAttemptsCollection.class);
    }
}
