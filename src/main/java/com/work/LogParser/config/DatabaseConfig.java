package com.work.LogParser.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class DatabaseConfig {

    // Читаем из переменных окружения с значениями по умолчанию
    public static final String DB_URL = System.getenv().getOrDefault(
            "SPRING_DATASOURCE_URL",
            "jdbc:postgresql://localhost:5432/ParserLog"
    );

    public static final String DB_USERNAME = System.getenv().getOrDefault(
            "SPRING_DATASOURCE_USERNAME",
            "postgres"
    );

    public static final String DB_PASSWORD = System.getenv().getOrDefault(
            "SPRING_DATASOURCE_PASSWORD",
            "uthgb123"
    );

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setFetchSize(1000);
        jdbcTemplate.setMaxRows(50000);
        return jdbcTemplate;
    }
}