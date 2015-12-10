package com.unionpaysmart.log4j.kafka;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * EventMessage 序列化
 * 
 * @author Kevin Huang
 * @version 0.0.1, 2015年11月24日 下午9:02:40
 */
public class EventMessageSerializer implements Serializer<EventMessage> {
    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("serializer.encoding");
        if (encodingValue != null && encodingValue instanceof String)
            encoding = (String) encodingValue;
    }

    @Override
    public byte[] serialize(String topic, EventMessage data) {
        try {
            if (data == null) {
                return null;
            } else {
                return data.toString().getBytes(encoding);
            }
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding " + encoding);
        }
    }

    @Override
    public void close() {
    }

}
