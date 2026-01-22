package com.work.LogParser.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class DatabaseConfig {

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        // Настройки для оптимальной производительности
        jdbcTemplate.setFetchSize(1000);
        jdbcTemplate.setMaxRows(50000);

        return jdbcTemplate;
    }
}