package com.kafka;

import org.apache.hadoop.util.ThreadUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;

/**
 * Created by Administrator on 2018/2/5.
 */
public class KafkaSendData {
    public static void main(String[] args) throws IOException {
        sendData();
    }

    public static void sendData() throws IOException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","slave3.hadoop:9092");
        properties.put("acks", "0");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        int cnt = 0;

        // 获取数据源
        String inpath = "E:\\bigData\\spark\\课件\\第10章Spark-Streaming\\logs\\hali.txt";
        // String inpath = "E:\\bigData\\spark\\课件\\第10章Spark-Streaming\\logs\\哈利.txt";
        FileInputStream fis = new FileInputStream(inpath);
        InputStreamReader isr = new InputStreamReader(fis,"gbk");
        BufferedReader br = new BufferedReader(isr);
        String line;
        while ((line = br.readLine()) != null){
            // 获取到的数据是以key : value的形式的
            ProducerRecord<String, String> message = new ProducerRecord<String, String>("hali","key：" + String.valueOf(cnt),line);
            kafkaProducer.send(message);
            System.out.println("发送消息：" + cnt++ + "：" + line);
            ThreadUtil.sleepAtLeastIgnoreInterrupts(500);
        }


    }

}
