package com.unionpaysmart.log4j.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.unionpaysmart.log4j.kafka.EventMessage;

/**
 *
 * @author Kevin Huang
 * @since 2015年11月6日 下午8:18:37
 */
public class ProducerDemo {
    private Producer<String, EventMessage> producer = null;
    public static final String TOPIC = "TEST-TOPIC";

    private ProducerDemo() {
        if (null == this.producer) {
            Properties props = new Properties();

            props.put(BOOTSTRAP_SERVERS_CONFIG, "warwick01:9092,warwick02:9092,warwick03:9092");
            props.put(ACKS_CONFIG, "1");
            props.put(BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024L);
            props.put(RETRIES_CONFIG, 1);
            props.put(VALUE_SERIALIZER_CLASS_CONFIG, "com.unionpaysmart.upsmart.log.EventMessageSerializer");
            props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<String, EventMessage>(props);
        }
    }
    
    public void sendBlock() {
        String key = String.valueOf("test-for-blocking-key");
        String data = "test-for-blocking-value";
        try {
            EventMessage message = new EventMessage("application", "level", "sss", data, "event.getThreadName()", 1l);
            RecordMetadata r = producer.send(new ProducerRecord<String, EventMessage>(TOPIC, key, message)).get();
            System.out.println("block send return: " + r.toString());
            producer.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void sendNonBlock() {
        String key = String.valueOf("test-for-non-blocking-key");
        String data = "test-for-non-blocking-value";
        EventMessage message = new EventMessage("application", "level", "sss", data, "event.getThreadName()", 1l);
        ProducerRecord<String, EventMessage> record = new ProducerRecord<String, EventMessage>(TOPIC, key, message);
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
                    e.printStackTrace();
                    System.out.println("The offset of the record we just sent is: " + metadata.offset());
                }
            }
        });
        producer.close();
    }

    public static void main(String[] args) {
        ProducerDemo demo = new ProducerDemo();
        demo.sendBlock();
//        demo.sendNonBlock();
    }
}

