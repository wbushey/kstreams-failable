package org.apache.kafka.failableTestSupport;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.failableTestSupport.FailableTestDrivers.FailableProcessorTestDriver;
import org.apache.kafka.failableTestSupport.extentions.TopologyPropertiesExtension;
import org.apache.kafka.failableTestSupport.mocks.mockCallbacks.MockCallback;
import org.apache.kafka.streams.kstream.internals.KStreamFailableForeach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;

import java.util.Properties;

@ExtendWith(TopologyPropertiesExtension.class)
public class WithFailableMockProcessorContext {
    protected FailableProcessorTestDriver<String, String> processorTestDriver;
    protected final ListAppender<ILoggingEvent> logAppender;
    private Logger foreachLogger = (Logger) LoggerFactory.getLogger(KStreamFailableForeach.class);

    public WithFailableMockProcessorContext(MockCallback<String, String> mockCallback, Properties topologyProps){
        final Serde<String> stringSerde = Serdes.String();
        this.processorTestDriver = new FailableProcessorTestDriver<>(mockCallback, topologyProps, stringSerde, stringSerde);
        logAppender = new ListAppender<>();
    }

    @BeforeEach
    public void addListAppender(){
        logAppender.start();
        foreachLogger.addAppender(logAppender);
    }


    @AfterEach
    public void removeListAppender(){
        foreachLogger.detachAppender(logAppender);
        logAppender.stop();
    }

}
