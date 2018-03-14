package com.kafka;

import com.utils.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by Administrator on 2018/1/20.
 */
public class GetMain {
    public static void main(String[] args){
        KafkaConsumer<String, String> kafkaConsumer = KafkaUtil.getConsumer("slave3.hadoop",9092);

        kafkaConsumer.subscribe(Arrays.asList("kg"));
        kafkaConsumer.seekToBeginning(new ArrayList());
        while(true){
            System.out.println("接受信息开始");
            ConsumerRecords<String,String> records = kafkaConsumer.poll(1000);
            for(ConsumerRecord<String, String> record:records){
                System.out.println("fetched from partition：" + record.partition());
                System.out.println(",offset：" + record.offset());
                System.out.println(",value：" + record.value());
            }
        }
    }
}
