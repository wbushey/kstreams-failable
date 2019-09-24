package org.apache.kafka.streams.kstream.internals.serialization.serdes;

import org.apache.kafka.streams.kstream.internals.models.TaskAttempt;

public class TaskAttemptSerde extends AbstractSerde<TaskAttempt>{
    public TaskAttemptSerde(){
        super(TaskAttempt.class);
    }
}
