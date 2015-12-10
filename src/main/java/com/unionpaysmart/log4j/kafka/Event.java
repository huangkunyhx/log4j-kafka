package com.unionpaysmart.log4j.kafka;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.spi.LoggingEvent;

/**
 * logging event and retry count
 * @author Kevin Huang
 * @version 0.0.1, 2015年11月26日 上午9:18:17
 */
public class Event {
    private LoggingEvent event;

    private AtomicInteger retries = new AtomicInteger(0);

    public Event(LoggingEvent event) {
        this.event = event;
    }

    public LoggingEvent getEvent() {
        return event;
    }

    public int incrementRetries() {
        return retries.incrementAndGet();
    }
}
