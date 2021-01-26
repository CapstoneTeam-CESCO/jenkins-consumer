package com.capstone.consumer.server;

import com.capstone.consumer.server.spark.SparkStartup;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

    public static void main(String[] args){
        SpringApplication springApplication = new SpringApplication(com.capstone.consumer.server.Application.class);
        springApplication.addListeners(new SparkStartup());
        springApplication.run(args);

    }
}
