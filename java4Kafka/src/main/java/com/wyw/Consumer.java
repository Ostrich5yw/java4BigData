package com.wyw;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @author 5yw
 * @date 2021/11/22 17:01
 */
public class Consumer {
    /**
     * KafkaConsumer：需要创建一个消费者对象，用来消费数据
     * ConsumerConfig：获取所需的一系列配置参数
     * ConsuemrRecord：每条数据都要封装成一个 ConsumerRecord 对象
     * 为了使我们能够专注于自己的业务逻辑，Kafka 提供了自动提交 offset 的功能。
     * 自动提交 offset 的相关参数：
     * enable.auto.commit：是否开启自动提交 offset 功能
     * auto.commit.interval.ms：自动提交 offset 的时间间隔
     *
     * */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("group.id", "test");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /**
         * 自动提交offset
         *  该配置应用于Comsumer初次连接或者掉线过久导致其存储的offset已经被删，对Consumer的offset进行恢复。配置为earliest的意思是：当group第一次连接到服务器时，
         *  将Consumer的offset配置为最早的offset
         *  offset存储的意义在于：如果Comsumer掉线，则该Comsumer从offset开始继续向后读取
         *
         *  如果希望读取历史数据，则应该配置该选项，并且重新命名一个group（已经有的group其offset已经存储了，无法读取）
         * */
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("first"));
        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());

            /**
             * 手动提交 offset
             * 方法有两种：分别是 commitSync（同步提交）和 commitAsync（异步
             * 提交）。两者的相同点是，都会将本次 poll 的一批数据最高的偏移量提交；不同点是，
             * commitSync 阻塞当前线程，一直到提交成功，并且会自动失败重试（由不可控因素导致，
             * 也会出现提交失败）；而 commitAsync 则没有失败重试机制，故有可能提交失败。
             *
             * */
//            consumer.commitSync();
//            consumer.commitAsync(new OffsetCommitCallback() {
//                @Override
//                public void onComplete(Map<TopicPartition,
//                                        OffsetAndMetadata> offsets, Exception exception) {
//                    if (exception != null) {
//                        System.err.println("Commit failed for" +
//                                offsets);
//                    }
//                }
//            });
        }
        //无论是同步提交还是异步提交 offset，都有可能会造成数据的漏消费或者重复消费。先
        //提交 offset 后消费，有可能造成数据的漏消费；而先消费后提交 offset，有可能会造成数据
        //的重复消费。
        /**
         * 自定义offset存储
         * offset 的维护是相当繁琐的，因为需要考虑到消费者的 Rebalace。
         * 当有新的消费者加入消费者组、已有的消费者退出消费者组或者所订阅的主题的分区发
         * 生变化，就会触发到分区的重新分配，重新分配的过程叫做 Rebalance。
         * 消费者发生 Rebalance 之后，每个消费者消费的分区就会发生变化。因此消费者要首先
         * 获取到自己被重新分配到的分区，并且定位到每个分区最近提交的 offset 位置继续消费。
         * */

//        Map<TopicPartition, Long> currentOffset = new HashMap<>();
//        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
//            //该方法会在 Rebalance 之前调用
//            @Override
//            public void
//            onPartitionsRevoked(Collection<TopicPartition> partitions) {
//                commitOffset(currentOffset);
//            }
//
//            //该方法会在 Rebalance 之后调用
//            @Override
//            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//                currentOffset.clear();
//                for (TopicPartition partition : partitions) {
//                    consumer.seek(partition, getOffset(partition));//定位到最近提交的 offset 位置继续消费
//                }
//            }
//        });
    }
    //获取某分区的最新 offset
    private static long getOffset(TopicPartition partition) {
        return 0;
    }
    //提交该消费者所有分区的 offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {

    }
}

