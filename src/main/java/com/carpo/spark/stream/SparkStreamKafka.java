package com.carpo.spark.stream;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Created by lg on 2018/5/8.
 */
public class SparkStreamKafka {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("main.java.computingCenter").setMaster("local");
        // Create the context with 2 seconds batch size
        //每两秒读取一次kafka
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, new Duration(10000));

        Map<String, String> kafkaParameters = new HashMap<String, String>();
        kafkaParameters.put("metadata.broker.list", "localhost:9092");

        Set<String> topics = new HashSet<String>();
        topics.add("test");

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jsc, String.class,
                String.class, StringDecoder.class, StringDecoder.class, kafkaParameters, topics);

        JavaDStream<String> words = messages.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            public Iterator<String> call(Tuple2<String, String> arg0)
                    throws Exception {
                return Arrays.asList(arg0._2().split(",")).iterator();
            }
        });
        //对其中的单词进行统计
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1)).reduceByKey((s1, s2) -> s1 + s2);
        wordCounts.print();

        /*
         * Spark
         * Streaming执行引擎也就是Driver开始运行，Driver启动的时候是位于一条新的线程中的，当然其内部有消息循环体，用于
         * 接受应用程序本身或者Executor中的消息；
         */
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jsc.close();
    }
}