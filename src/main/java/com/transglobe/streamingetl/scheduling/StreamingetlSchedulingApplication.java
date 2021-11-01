package com.transglobe.streamingetl.scheduling;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class StreamingetlSchedulingApplication {

	public static void main(String[] args) {
		SpringApplication.run(StreamingetlSchedulingApplication.class);
	}

}
