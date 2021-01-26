package com.capstone.consumer.server.spark;

import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.boot.context.event.ApplicationStartingEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

public class SparkStartup implements ApplicationListener<ContextRefreshedEvent> {

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        ApplicationContext ac = event.getApplicationContext();
        SparkRunner sparkKafkaStreamExecutor= ac.getBean(SparkRunner.class);
        Thread thread = new Thread(sparkKafkaStreamExecutor);
        thread.start();
    }

}