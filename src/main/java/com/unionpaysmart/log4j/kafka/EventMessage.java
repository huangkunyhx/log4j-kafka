package com.unionpaysmart.log4j.kafka;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;

/**
 *
 * @author Kevin Huang
 * @version 0.0.1, 2015年11月6日 下午1:55:35
 */
public class EventMessage implements Serializable {
    private static final long serialVersionUID = 692822303063679213L;

    private String application;
    /**
     * Level of logging event.
     */
    private String level;
    /**
     * The class (logger) name.
     */
    private String className;
    /**
     * The application supplied message of logging event.
     */
    private String message;

    /**
     * The name of thread in which this logging event was generated.
     */
    private String threadName;

    /**
     * Return this event's throwable's string list representaion.
     */
    private List<String> throwableInfo;

    /**
     * the method name of the caller.
     */
//    private String method;

    /**
     * the line number of the caller.
     */
//    private String line;

    /**
     * the NDC for this event.
     */
    private String stack;

    /**
     * The number of milliseconds elapsed from 1/1/1970 until logging event was
     * created.
     */
    private long timeStamp;

    public EventMessage() {
        super();
    }

    public EventMessage(String application, String level, String className, String message, String threadName, long timeStamp) {
        this();
        this.application = application;
        this.level = level;
        this.className = className;
        this.message = message;
        this.threadName = threadName;
        this.timeStamp = timeStamp;
    }

    public EventMessage(String application, LoggingEvent loggingEvent, Layout layout) {
        this();
        this.application = application;
        this.level = loggingEvent.getLevel().toString();
        this.className = loggingEvent.getLogger().getName();
        this.message = loggingEvent.getRenderedMessage();
        this.threadName = loggingEvent.getThreadName();
        this.timeStamp = loggingEvent.getTimeStamp();
        this.stack = loggingEvent.getNDC();

        if (layout.ignoresThrowable()) {
            String[] s = loggingEvent.getThrowableStrRep();
            if (s != null) {
                this.throwableInfo = Arrays.asList(s);
            }
        }
    }

    public String getApplication() {
        return application;
    }

    public void setApplication(String application) {
        this.application = application;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public List<String> getThrowableInfo() {
        return throwableInfo;
    }

    public void setThrowableInfo(List<String> throwableInfo) {
        this.throwableInfo = throwableInfo;
    }

    public String getStack() {
        return stack;
    }

    public void setStack(String stack) {
        this.stack = stack;
    }

    @Override
    public String toString() {
        StringBuilder builder =  new StringBuilder();

        builder.append('{')
            .append("\"application\":").append(getJsonValue(application)).append(',')
            .append("\"level\":").append(getJsonValue(level)).append(',')
            .append("\"className\":").append(getJsonValue(className)).append(',')
            .append("\"message\":").append(getJsonValue(message)).append(',')
            .append("\"threadName\":").append(getJsonValue(threadName)).append(',')
            .append("\"stack\":").append(getJsonValue(stack)).append(',')
            .append("\"timeStamp\":").append(timeStamp).append(',')
            .append("\"throwableInfo\":").append(getJsonArrayValue(throwableInfo))
            .append('}');

         return builder.toString();
    }

    private StringBuilder getJsonValue(String value) {
        if (null == value || value.trim().equals("")) {
            return null;
        }
        StringBuilder builder =  new StringBuilder();
        builder.append("\"").append(value).append("\"");
        return builder;
    }

    private StringBuilder getJsonArrayValue(List<String> value) {
        if (null == value || value.isEmpty()) {
            return null;
        }

        int len = value.size();
        StringBuilder builder =  new StringBuilder();
        builder.append("[").append("\"").append(value.get(0)).append("\"");
        for (int i = 1; i < len; i++) {
            String item = value.get(i);
            builder.append(",\"").append(item).append("\"");
        }
        builder.append("]");
        return builder;
    }

}
