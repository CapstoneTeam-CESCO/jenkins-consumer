package com.capstone.consumer.server.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {
    @Value("${spark.driver.bindAddress}")
    private String sparkDriver;
    @Value("${spark.app.name}")
    private String sparkAppName;
    @Value("${spark.master}")
    private String sparkMaster;
    @Value("${es.index.auto.create}")
    private String esIndexAutoCreate;
    @Value("${es.nodes.wan.only}")
    private String esNodeWanOnly;
    @Value("${es.nodes}")
    private String esNodes;
    @Value("${es.port}")
    private String esPort;
    @Value("${es.input.json}")
    private String esInputJson;


// TODO: 차후 필요할만한 OPTIONS

//    @Value("${spark.stream.kafka.durations}")
//    private String streamDurationTime;
//    @Value("${spark.driver.memory}")
//    private String sparkDriverMemory;
//    @Value("${spark.worker.memory}")
//    private String sparkWorkerMemory;
//    @Value("${spark.executor.memory}")
//    private String sparkExecutorMemory;
//    @Value("${spark.rpc.message.maxSize}")
//    private String sparkRpcMessageMaxSize;


    @Bean
    @ConditionalOnMissingBean(SparkConf.class)
    public SparkConf sparkConf() {
        SparkConf conf = new SparkConf()
                .setMaster("local[3]")  // .setMaster("local[*]");//just use in test
                .setAppName(sparkAppName)
                .set("spark.driver.bindAddress",sparkDriver)
        .set("es.index.auto.create",esIndexAutoCreate)
        .set("es.nodes.wan.only",esNodeWanOnly)
        .set("es.nodes",esNodes)
        .set("es.port", esPort)
        .set("es.input.json", esInputJson);

//        .set("spark.driver.memory",sparkDriverMemory)
//                .set("spark.worker.memory",sparkWorkerMemory)//"26g".set("spark.shuffle.memoryFraction","0") //默认0.2
//                .set("spark.executor.memory",sparkExecutorMemory)
//                .set("spark.rpc.message.maxSize",sparkRpcMessageMaxSize);

        return conf;
    }

    @Bean
    @ConditionalOnMissingBean(JavaSparkContext.class)
    public JavaSparkContext javaSparkContext(@Autowired SparkConf sparkConf) {
        return new JavaSparkContext(sparkConf);
    }

}
