package com.wyw;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author 5yw
 * @date 2021/11/22 20:15
 */
public class diyProducer {
    public static void main(String[] args){
        // 构建Kafka配置文件
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        //添加拦截器
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, new ArrayList<String>(){
            {
                add("com.wyw.diyInterceptor");
                add("com.wyw.diy2Interceptor");
            }
        });
        // 创建生产者对象
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        for (int i = 1; i <= 10; i++) {
            producer.send(new ProducerRecord<String, String>("first",
                    "my message " + Integer.toString(i)));
        }
        producer.close();
    }
}
