package com.invent.optimus.OptimusSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${spark.app.name}")
    private String appName;

    @Value("${spark.master}")
    private String masterUri;

    @Bean
    public SparkConf conf() {
        return new SparkConf().setAppName(appName)
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");
    }

    @Bean
    public SparkSession ss() {
        return SparkSession.builder().config(conf()).getOrCreate();
    }

}
