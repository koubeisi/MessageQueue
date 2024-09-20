package com.koubs.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;

/**
 * @author PC
 * @since 2024/9/19
 */
@Slf4j
public class KafkaProducerTest {

    public static void main(String[] args) {


        var configMap = new HashMap<String, Object>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "121.37.20.182:9090");
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 配置确认机制: "acks=all" 表示消息会在所有同步副本收到后返回成功确认
        configMap.put(ProducerConfig.ACKS_CONFIG, "all");  // 可选值："0", "1", "all"

        configMap.put(ProducerConfig.RETRIES_CONFIG, 5);  // 设置重试次数:如果5次都失败了只会产生一个异常
        configMap.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");    // 开启幂等性:设置了重试最好开启

        var producer = new KafkaProducer<String, String>(configMap);

        for (int i = 0; i < 10; i++) {
            var producerRecord = new ProducerRecord<>("test-topic", "key", "value");
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    // 消息发送成功
                    System.out.println("Message sent to partition " + metadata.partition() + " with offset " + metadata.offset());
                } else {
                    // 消息发送失败
                    exception.printStackTrace();
                    // 可以将消息存入其它地方(死信队列)
                }
            });
        }
        log.info("{}", "send message success");
        producer.close();
    }
}
