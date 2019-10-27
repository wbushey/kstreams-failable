package org.apache.kafka.failableTestSupport.assertions;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LogAssertions {
    private final ListAppender<ILoggingEvent> logAppender;

    /**
     * Decorates a ListAppender with methods that assert expectations.
     * @param logAppender
     * @return
     */
    public static LogAssertions expect(ListAppender<ILoggingEvent> logAppender){
        return new LogAssertions(logAppender);
    }

    public void toHaveLoggedExactly(Level level, String message){
        assertEquals(1, logAppender.list.size());

        assertEquals(level, logAppender.list.get(0).getLevel());
        assertEquals(message, logAppender.list.get(0).getMessage());
    }

    private LogAssertions(ListAppender<ILoggingEvent> logAppender){
        this.logAppender = logAppender;
    }
}
