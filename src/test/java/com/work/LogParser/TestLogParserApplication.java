package com.work.LogParser;

import org.springframework.boot.SpringApplication;

public class TestLogParserApplication {

	public static void main(String[] args) {
		SpringApplication.from(LogParserApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
