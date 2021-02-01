package com.capstone.consumer.server.spark;

import com.capstone.consumer.server.common.LogUtil;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601DateFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.receiver.Receiver;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

@Component
public class SparkRunner implements Serializable,Runnable {


//        stream.mapToPair( // Using lambda Expression
//                (PairFunction<ConsumerRecord<String, String>, String, String>) record -> new Tuple2<String,String>(record.key(),record.value()));

//        stream.mapToPair(
//                new PairFunction<ConsumerRecord<String,String>, String, String>(){
//                    public Tuple2<String,String> call(ConsumerRecord<String,String> record){
//                        return new Tuple2<String,String>(record.key(),record.value());
//                    }
//                });

    @Value("${spark.kafka.topics}")
    private  String sparkKafkaTopics;
    @Value("${bootstrap.servers}")
    private String bootstrapServers;
    @Value("${key.serializer}")
    private String keySerializer;
    @Value("${key.deserializer}")
    private String keyDeserializer;
    @Value("${value.serializer}")
    private String valueSerializer;
    @Value("${value.deserializer}")
    private String valueDeserializer;
    @Value("${group.id}")
    private String groupId;
    @Value("${auto.offset.reset}")
    private String autoOffsetReset;
    @Value("${enable.auto.commit}")
    private String enableAutoCommit;


    private transient JavaStreamingContext jsc;

    @Autowired
    private transient JavaSparkContext javaSparkContext;

    @Override
    public void run() {
            sparkStreaming();
    }

    public void sparkStreaming(){

        jsc = new JavaStreamingContext(javaSparkContext, Durations.seconds(1));
        Collection<String> topics = Arrays.asList(sparkKafkaTopics);

        //TODO: 카프카 Config 분리
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootstrapServers);
        kafkaParams.put("key.serializer", keySerializer);
        kafkaParams.put("key.deserializer",keyDeserializer);
        kafkaParams.put("value.serializer", valueSerializer);
        kafkaParams.put("value.deserializer",valueDeserializer);
        kafkaParams.put("group.id",groupId);
        kafkaParams.put("auto.offset.reset",autoOffsetReset);
        kafkaParams.put("enable.auto.commit",enableAutoCommit);


        JavaInputDStream<ConsumerRecord<String, Object>> stream =
                KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, Object>Subscribe(topics, kafkaParams));

        // Spark Streaming 읽은 데이터의 value 를 출력한다.
        stream.map(raw->raw.value()).print();

        // Elastalert 사용을 위한 Timestamp 를 생성한다.
        Date now = new Date(System.currentTimeMillis());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("CET"));
        String strNow = sdf.format(now);

        // 데이터를 json 형식의 문자열로 변환하여 elasticsearch 에 저장한다.
        JavaDStream<String> finalStream = stream.map(new Function<ConsumerRecord<String, Object>, String>() {
            @Override
            public String call(ConsumerRecord<String, Object> cr) throws SQLException {
                String json = "{" +
                        "\"timestamp\":\"" + strNow + "\"," +
                        "\"index\":{" +
                        "\"data\":" + (String)cr.value() +
                        "}}";
                return json;
            }
        });


        JavaEsSparkStreaming.saveJsonToEs(finalStream, "streaming/text");
        LogUtil.traceLog.info("Start Spark Streaming & elasticsearch");


        jsc.start();
        try {
            jsc.awaitTermination();
        }catch (Exception e){
            LogUtil.traceLog.info(e.getMessage());
        }
    }
}
