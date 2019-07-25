package com.invent.optimus.OptimusSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
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
//                .set("textinputformat.record.delimiter", "\t")
                .setMaster("local");
    }

//    @Bean
//    public SparkContext sc() {
//        return new SparkContext(conf());
//    }

    @Bean
    public SparkSession ss() {
        return SparkSession.builder().config(conf()).getOrCreate();
    }

}
