package com.invent.optimus.OptimusSpark;

import com.invent.optimus.OptimusSpark.service.SparkService;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class OptimusSparkApplication implements CommandLineRunner {

	@Autowired
	private SparkService sparkService;

	public static void main(String[] args) {
		SpringApplication.run(OptimusSparkApplication.class, args);
	}


	@Override
	public void run(String... args) throws Exception {
		sparkService.process();
	}
}
