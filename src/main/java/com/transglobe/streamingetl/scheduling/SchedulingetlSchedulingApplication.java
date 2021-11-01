package com.transglobe.streamingetl.scheduling;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SchedulingetlSchedulingApplication {

	public static void main(String[] args) {
		SpringApplication.run(SchedulingetlSchedulingApplication.class);
	}

}
