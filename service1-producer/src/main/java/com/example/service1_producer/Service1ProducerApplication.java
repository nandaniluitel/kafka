package com.example.service1_producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class Service1ProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(Service1ProducerApplication.class, args);
	}

}
