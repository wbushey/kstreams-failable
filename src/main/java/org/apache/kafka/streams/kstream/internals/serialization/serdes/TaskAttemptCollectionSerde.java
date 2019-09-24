package org.apache.kafka.streams.kstream.internals.serialization.serdes;

import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;

import java.util.Collection;

public class TaskAttemptCollectionSerde extends AbstractSerde<Collection<TaskAttempt>> {
    public TaskAttemptCollectionSerde(){
        super(Class<Collection<TaskAttempt>>);
    }
}
