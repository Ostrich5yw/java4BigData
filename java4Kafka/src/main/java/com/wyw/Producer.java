package com.wyw;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author 5yw
 * @date 2021/11/19 16:10
 */
public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 构建Kafka配置文件
        Properties props = new Properties();
        //kafka 集群，broker-list
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.put("bootstrap.servers", "hadoop102:9092");
//        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put("acks", "all");
        //重试次数
        props.put("retries", 1);
        //批次大小
        props.put("batch.size", 16384);
        //等待时间
        props.put("linger.ms", 1);
        //RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者对象
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        /**
         * 不带回调函数的发送消息
         *
         * */
        for (int i = 1; i <= 10; i++) {
            producer.send(new ProducerRecord<String, String>("first",
                    "my message " + Integer.toString(i)));
        }
        /**
         * 带回调函数的发送消息
         * 回调函数会在 producer 收到 ack 时调用，为异步调用，该方法有两个参数，分别是
         * RecordMetadata 和 Exception，如果 Exception 为 null，说明消息发送成功，如果
         * Exception 不为 null，说明消息发送失败。
         * */
        for (int i = 1; i <= 10; i++) {
            producer.send(new ProducerRecord<String, String>("second",
                    Integer.toString(i), Integer.toString(i)), new Callback() {
                //回调函数，该方法会在 Producer 收到 ack 时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata metadata,
                                         Exception exception) {
                    if (exception == null) {
                        System.out.println("分区：" + metadata.partition() + "偏移量：" +
                                metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }
        /**
         * 实现消息同步发送（一条消息发送成功后再发送下一条）：通过future类的get方法，只有sender方法调用成功，get()才会停止对主线程的阻塞，程序才会继续运行
         * */
        for (int i = 1; i <= 10; i++) {
            producer.send(new ProducerRecord<String, String>("first",
                    Integer.toString(i), Integer.toString(i))).get();
        }
        // 关闭资源
        producer.close();
    }
}
