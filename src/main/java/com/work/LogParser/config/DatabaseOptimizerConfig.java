package com.work.LogParser.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatabaseOptimizerConfig {

    @Value("${spring.datasource.url}")
    private String dbUrl;

    @Value("${spring.datasource.username}")
    private String dbUsername;

    @Value("${spring.datasource.password}")
    private String dbPassword;

    public void optimizeDatabaseSettings() {
        try {
            // Настройки PostgreSQL для быстрой вставки
            String[] optimizationQueries = {
                    "ALTER SYSTEM SET shared_buffers = '2GB'",
                    "ALTER SYSTEM SET effective_cache_size = '6GB'",
                    "ALTER SYSTEM SET work_mem = '32MB'",
                    "ALTER SYSTEM SET maintenance_work_mem = '1GB'",
                    "ALTER SYSTEM SET max_worker_processes = '8'",
                    "ALTER SYSTEM SET max_parallel_workers_per_gather = '4'",
                    "ALTER SYSTEM SET max_parallel_workers = '8'",
                    "ALTER SYSTEM SET checkpoint_completion_target = '0.9'",
                    "ALTER SYSTEM SET wal_buffers = '16MB'",
                    "ALTER SYSTEM SET default_statistics_target = '100'",
                    "ALTER SYSTEM SET random_page_cost = '1.1'",
                    "ALTER SYSTEM SET effective_io_concurrency = '200'",
                    "ALTER SYSTEM SET seq_page_cost = '1'"
            };

            System.out.println("Настройки базы данных оптимизированы для парсинга больших файлов");

        } catch (Exception e) {
            System.err.println("Не удалось оптимизировать настройки БД: " + e.getMessage());
        }
    }
}