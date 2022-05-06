package com.wyw;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author 5yw
 * @date 2021/11/22 20:12
 */
public class diy2Interceptor implements ProducerInterceptor {
    int success=0;
    int error=0;
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(metadata != null){
            success++;
        }else{
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("success Num:"+success);
        System.out.println("error Num:"+error);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
