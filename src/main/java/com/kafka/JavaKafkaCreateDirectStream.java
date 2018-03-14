//package com.kafka;
//
//import org.apache.commons.collections.CollectionUtils;
//import org.apache.commons.lang.StringUtils;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.spark.SparkConf;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.api.java.JavaInputDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.*;
//import scala.Tuple2;
//
//import java.util.*;
//
///**
// * Created by Administrator on 2018/2/24.
// */
//public class JavaKafkaCreateDirectStream {
//    public static Map<String,Object> initKafkaConsumerConf(){
//        Map<String,Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("bootstrap.servers","slave3.hadoop:9092");
//        kafkaParams.put ("key.deserializer", StringDeserializer.class);
//        kafkaParams.put ("value.deserializer", StringDeserializer.class) ;
//        kafkaParams.put ("group.id","words-top-search");
//        kafkaParams.put ("auto.offset.reset","latest");
//        kafkaParams.put ("enable.auto.commit", false);
//        return kafkaParams;
//    }
//
//    public static void println(List<Tuple2<String, Long>> wordCountList){
//        if(CollectionUtils.isNotEmpty(wordCountList)){
//            List<Tuple2<String, Long>> sortList = new ArrayList<Tuple2<String, Long>>(wordCountList);
//            sortList.sort(new Comparator<Tuple2<String, Long>>() {
//                @Override
//                public int compare(Tuple2<String, Long> t1, Tuple2<String, Long> t2) {
//                    if(t2._2.compareTo(t1._2) > 0){
//                        return 1;
//                    }else if(t2._2.compareTo(t1._2) < 0){
//                        return -1;
//                    }
//                    return 0;
//                }
//            });
//            System.out.println("===============================");
//            for(Tuple2<String, Long> wordCount : sortList){
//                System.out.println(wordCount._1 + ":" + wordCount._2);
//            }
//            System.out.println("===============================");
//        }
//    }
//
//    public static void main(String[] args) throws InterruptedException {
//        SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaCreateDirectStream").setMaster(
//                "spark://master1.hadoop:7077");
//        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf,new Duration(10000));
//        Map<String, Object> kafkaParams = initKafkaConsumerConf();
//        streamingContext.checkpoint("/words-top-search");
//        final JavaInputDStream<ConsumerRecord<String, String>> inputDStream = KafkaUtils.createDirectStream(
//                streamingContext,
//                LocationStrategies.PreferConsistent(),
//                ConsumerStrategies.<String, String>Subscribe(Arrays.asList("words-search"),kafkaParams));
//
//        // 读取单词切分成元组
//        JavaPairDStream<String, String> keyWords = inputDStream.mapToPair(record -> {
//            return new Tuple2<String, String>(StringUtils.trimToEmpty(record.value()),
//                    StringUtils.trimToEmpty(record.value()));
//        });
//
//        keyWords.map((value) -> value._2()).filter((word) -> {
//            if(StringUtils.isBlank(word)){
//                return false;
//            }
//            return true;
//        }).countByValueAndWindow(new Duration(5*60*1000),new Duration(5*60*1000))
//                .foreachRDD(records -> {println(records.sortByKey(false).take(10));});
//        streamingContext.start();
//        streamingContext.awaitTermination();
//    }
//
//
//}
