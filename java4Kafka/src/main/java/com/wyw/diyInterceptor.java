package com.wyw;

import lombok.val;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author 5yw
 * @date 2021/11/22 19:56
 */
public class diyInterceptor implements ProducerInterceptor {

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        //1. 取出数据
        val value = record.value();

        //2. 创建一个新对象

        return new ProducerRecord<>(record.topic(), record.partition(), record.key(), System.currentTimeMillis() + "," + record.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
