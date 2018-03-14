package com.utils;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * kafka工具类
 * Created by Administrator on 2018/1/20.
 */
public class KafkaUtil {

    /**
     * 获取kafka生产者实例
     * @param ip 服务器ip
     * @param port 推送端口
     * @return 已打开的生产者实例
     */
    public static KafkaProducer<String,String> getProducer(String ip, int port){
        if(ip == null && port < 0){
            return null;
        }

        Properties pro = new Properties();
        pro.put("bootstrap.servers", ip+":"+port);
        pro.put("acks", "0");
        pro.put("retries", 0);
        pro.put("batch.size", 16384);
        pro.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pro.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(pro);
        return kafkaProducer;
    }


    public static KafkaConsumer<String, String> getConsumer(String ip, int port){
        if(ip == null && port < 0){
            return null;
        }
        Properties pro = new Properties();
        pro.put("bootstrap.servers", ip+":"+port);
        pro.put("group.id", "12");
        pro.put("enable.auto.commit", "true");
        pro.put("auto.commit.interval.ms", "1000");
        pro.put("session.timeout.ms","30000");
        pro.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        pro.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(pro);
        return kafkaConsumer;

    }

}
