package com.koubs.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;

/**
 * @author PC
 * @since 2024/9/19
 */
public class KafkaConsumerTest {

    public static void main(String[] args) {


        var configMap = new HashMap<String, Object>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "121.37.20.182:9090");
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

        // 禁用自动提交确认
        configMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 如果设置自动提取确认为true,可以设置自动提交的时间间隔
//        configMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // 一次最多拉取500条数据,增大 max.poll.records 可以减少消费者调用 poll() 的频率，从而提高吞吐量
        configMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");


        var consumer = new KafkaConsumer<String, String>(configMap);

        consumer.subscribe(List.of("test-topic"));

        // 拉取数据
        while (true){
            var records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> data : records) {
                // 此处业务逻辑必须是幂等的, 否则可能会导致数据重复消费
                System.out.println(data);
            }
            // 同步提交偏移量
            consumer.commitSync();  // 确保在消费完消息后同步提交
        }
//        consumer.close();

    }

}
