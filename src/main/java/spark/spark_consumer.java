package spark;

import common.LogUtil;
import mariaConnect.jdbcConnector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


@SpringBootApplication
public class spark_consumer {

    public static void main(String[] args) throws InterruptedException, SQLException {
        SpringApplication.run(spark_consumer.class, args);

        LogUtil.traceLog.info(("JDBC Connection..."));
        jdbcConnector.connectDB();

        LogUtil.traceLog.info("Spark Streaming & elasticsearch setting...");
        SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("streaming");
        conf.set("spark.driver.bindAddress", "127.0.0.1");
        conf.set("es.index.auto.create", "true");
        conf.set("es.nodes.wan.only", "true");
        conf.set("es.nodes", "34.64.119.246");
        conf.set("es.port", "9200");
        conf.set("es.input.json", "true");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(1)); // Create StreamingContext which can manage the creation and operating of Streaming

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "34.64.155.139:9092");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id","spark_id");
        kafkaParams.put("auto.offset.reset","latest");
        kafkaParams.put("enable.auto.commit",false);

        Collection<String> topics = Arrays.asList("test-topic");

        JavaInputDStream<ConsumerRecord<String, Object>> stream =
                KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, Object>Subscribe(topics, kafkaParams)
                );

//        stream.mapToPair( // Using lambda Expression
//                (PairFunction<ConsumerRecord<String, String>, String, String>) record -> new Tuple2<String,String>(record.key(),record.value()));

//        stream.mapToPair(
//                new PairFunction<ConsumerRecord<String,String>, String, String>(){
//                    public Tuple2<String,String> call(ConsumerRecord<String,String> record){
//                        return new Tuple2<String,String>(record.key(),record.value());
//                    }
//                });

        // Spark Streaming 읽은 데이터의 value 를 출력한다.
        stream.map(raw->raw.value()).print();

        //mariaDB connect


        // 데이터를 json 형식의 문자열로 변환하여 elasticsearch 에 저장한다.
        JavaDStream<String> finalStream = stream.map(new Function<ConsumerRecord<String, Object>, String> () {
            @Override
            public String call(ConsumerRecord<String, Object> cr) throws SQLException {
                String json = "{" +
                        "\"index\":{" +
                        "\"data\":" + (String)cr.value() +
                        "}}";
                jdbcConnector.insert((String)cr.value());
                    return json;
                }
            }
        );
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
