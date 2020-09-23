package spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
//import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import common.LogUtil;

@SpringBootApplication
public class spark_consumer {

    public static void main(String[] args) throws InterruptedException{
        SpringApplication.run(spark_consumer.class, args);

        LogUtil.traceLog.info("Start Spark Streaming");
        SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("SPARK-Streaming"); // Save some settings
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10)); // Create StreamingContext which can manage the creation and operating of Streaming

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "34.64.120.38:9092");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id","spark_id");
        kafkaParams.put("auto.offset.reset","latest");
        kafkaParams.put("enable.auto.commit",false);

        Collection<String> topics = Arrays.asList("test-par3");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(jsc,LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        stream.mapToPair( // Using lambda Expression
                (PairFunction<ConsumerRecord<String, String>, String, String>) record -> new Tuple2<String,String>(record.key(),record.value()));

//        stream.mapToPair(
//                new PairFunction<ConsumerRecord<String,String>, String, String>(){
//                    public Tuple2<String,String> call(ConsumerRecord<String,String> record){
//                        return new Tuple2<String,String>(record.key(),record.value());
//                    }
//                });

        stream.map(raw->raw.value()).print();

        jsc.start();
        jsc.awaitTermination();
    }
}
