package org.marcusbb.queue.kafka.utils;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jim.lin on 7/5/2017.
 *
 * This is a log appender for unit testing purpose.
 */
public class TestAppender extends AppenderSkeleton {
    private final List<LoggingEvent> log = new ArrayList<LoggingEvent>();

    @Override
    public boolean requiresLayout() {
        return false;
    }

    @Override
    protected void append(final LoggingEvent loggingEvent) {
        log.add(loggingEvent);
    }

    @Override
    public void close() {
    }

    public List<LoggingEvent> getLog() {
        return new ArrayList<LoggingEvent>(log);
    }
}
