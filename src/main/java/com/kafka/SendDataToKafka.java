package com.kafka;

import org.apache.hadoop.util.ThreadUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by Administrator on 2018/1/20.
 */
public class SendDataToKafka {

    public static void main(String[] args) {
        sendData();
    }

    public static void sendData(){
        Properties pro  = new Properties();
        String kafakIp = "slave3.hadoop";
        String kafkaPort = "9092";
        pro.put("bootstrap.servers",kafakIp+":"+kafkaPort);
        pro.put("acks", "0");
        pro.put("retries", 0);
        pro.put("batch.size", 16384);
        pro.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pro.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 生产者
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(pro);
        int cnt = 0;
        while (true){
            if(cnt > 100){
                break;
            }
            ProducerRecord<String, String> message = new ProducerRecord<String, String>("myMessage",String.valueOf(cnt),"第"+String .valueOf(cnt)+"消息！");
            kafkaProducer.send(message);
            System.out.println("第"+String .valueOf(cnt)+"消息！");
            cnt = cnt + 1;
            // ThreadUtil.sleepAtLeastIgnoreInterrupts(1000*2);
        }

    }

}
