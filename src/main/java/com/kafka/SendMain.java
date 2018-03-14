package com.kafka;

import com.utils.KafkaUtil;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * kafka发送消息实例
 * Created by Administrator on 2018/1/20.
 */
public class SendMain {
    public static void main(String[] args){
        KafkaProducer<String, String> kafkaProducer = KafkaUtil.getProducer("slave3.hadoop",9092);
        int cnt = 0;
        while (cnt < 10){
            cnt++;
            ProducerRecord<String, String> message = new ProducerRecord("kg", String.valueOf(cnt),"this is message "+ cnt);
            kafkaProducer.send(message);
            System.out.println("发送消息：" + cnt);
           ThreadUtil.sleepAtLeastIgnoreInterrupts(1000*2);

        }
    }
}
