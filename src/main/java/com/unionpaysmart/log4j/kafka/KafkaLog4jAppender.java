package com.unionpaysmart.log4j.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;

/**
 *
 * @author Kevin Huang
 * @version , 2015年11月6日 上午10:56:15
 */
public class KafkaLog4jAppender extends AppenderSkeleton {
    /**
     * the project name
     */
    private String application = null;

    /**
     * the topic where log events add to.
     */
    private String topic = null;

    /**
     * A list of host/port pairs to use for establishing the initial connection
     * to the Kafka cluster. <code>host1:port1,host2:port2,...</code>
     */
    private String servers = null;

    /**
     * The number of acknowledgments the producer requires the leader to have
     * received before considering a request complete. This controls the
     * durability of records that are sent. The following settings are common:
     * acks=0, 1, all
     */
    private String acks = null;

    /**
     * The total bytes of memory the producer can use to buffer records waiting
     * to be sent to the server.
     */
    private Long bufferMemory = 32 * 1024 * 1024L;

    /**
     * Setting a value greater than zero will cause the client to resend any
     * record whose send fails with a potentially transient error.
     */
    private int retries = 0;
    /**
     * Serializer class for value that implements the <code>Serializer</code>
     * interface.
     */
    private String valueSerializer = "com.unionpaysmart.log4j.kafka.EventMessageSerializer";

    /**
     * Serializer class for key that implements the <code>Serializer</code>
     * interface.
     */
    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";

    /**
     * Where LoggingEvents are queued to send.
     */
    private LinkedBlockingQueue<Event> events = new LinkedBlockingQueue<Event>();

    /**
     * The pool of senders.
     */
    private ThreadPoolExecutor senderPool = null;

    /**
     * Used to synchronize access when creating the kafka.
     */
    private final String mutex = "mutex";

    /**
     * How many senders to use at once. Use more senders if you have lots of log
     * output going through this appender.
     */
    private int senderPoolSize = 3;

    /**
     * How many times to retry sending a message if the broker is unavailable or
     * there is some other error.
     */
    private int maxSenderRetries = 30;

    /**
     * kafka producer.
     */
    private Producer<String, EventMessage> kafkaProducer;

    public String getApplication() {
        return application;
    }

    public void setApplication(String application) {
        this.application = application;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public Long getBufferMemory() {
        return bufferMemory;
    }

    public void setBufferMemory(Long bufferMemory) {
        this.bufferMemory = bufferMemory;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public void setValueSerializer(String valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
    }

    public KafkaLog4jAppender() {
        super();
    }

    public KafkaLog4jAppender(boolean isActive) {
        super(isActive);
    }

    @Override
    public void close() {
        closed = true;
        if (null != senderPool) {
            senderPool.shutdownNow();
            senderPool = null;
        }

        if (null != kafkaProducer) {
            kafkaProducer.close();
        }
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    @Override
    public void activateOptions() {
        super.activateOptions();
    }

    @Override
    protected void append(LoggingEvent loggingEvent) {
        if (null == senderPool) {
            synchronized (mutex) {
                if (null == senderPool) {
                    startSenders();
                    Properties props = new Properties();
                    props.put(BOOTSTRAP_SERVERS_CONFIG, servers);
                    props.put(ACKS_CONFIG, acks);
                    props.put(BUFFER_MEMORY_CONFIG, bufferMemory);
                    props.put(RETRIES_CONFIG, retries);
                    props.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
                    props.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer);

                    kafkaProducer = new KafkaProducer<String, EventMessage>(props);
                }
            }
        }

        events.add(new Event(loggingEvent));

        if (senderPoolSize > 1) {
            synchronized (senderPool) {
                if (senderPool.getActiveCount() < senderPoolSize) {
                    senderPool.submit(new EventSender());
                }
            }
        } else {
            if (senderPool.getActiveCount() < senderPoolSize) {
                senderPool.submit(new EventSender());
            }
        }
    }

    /**
     * Submit the required number of senders into the pool.
     */
    protected void startSenders() {
        senderPool = new ThreadPoolExecutor(0, senderPoolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    }

    /**
     * Helper class to actually send LoggingEvents asynchronously.
     */
    protected class EventSender implements Runnable {
        @Override
        public void run() {
            if (null == kafkaProducer) {
                return;
            }

            try {
                while (!events.isEmpty()) {
                    final Event event = events.poll();
                    if (null == event) {
                        break;
                    }

                    LoggingEvent loggingEvent = event.getEvent();
                    EventMessage message = new EventMessage(application, loggingEvent, layout);
                    ProducerRecord<String, EventMessage> record = new ProducerRecord<String, EventMessage>(topic, message);

                    // Send a message
                    try {
                        kafkaProducer.send(record);
                    } catch (Exception e) {
                        int retries = event.incrementRetries();
                        if (retries < maxSenderRetries) {
                            events.add(event);
                        } else {
                            errorHandler.error("Could not send log message " + loggingEvent.getRenderedMessage()
                                    + " after " + maxSenderRetries + " retries", e, ErrorCode.WRITE_FAILURE,
                                    loggingEvent);
                        }
                    }

                }
            } catch (Throwable t) {
                throw new RuntimeException(t.getMessage(), t);
            }

        }
    }

}
