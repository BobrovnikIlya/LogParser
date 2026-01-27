package com.work.LogParser.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class DatabaseConfig {

    public static final String DB_URL = "jdbc:postgresql://localhost:5432/ParserLog";
    public static final String DB_USERNAME = "postgres";
    public static final String DB_PASSWORD = "uthgb123";

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        // Настройки для оптимальной производительности
        jdbcTemplate.setFetchSize(1000);
        jdbcTemplate.setMaxRows(50000);

        return jdbcTemplate;
    }
}