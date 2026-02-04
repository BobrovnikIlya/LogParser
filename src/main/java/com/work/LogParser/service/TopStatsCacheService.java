package com.work.LogParser.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Service
public class TopStatsCacheService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // Создаем материализованные представления с автообновлением
    public void createTopStatsTables(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            // 1. Топ URL
            st.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_urls AS
                SELECT 
                    url,
                    COUNT(*) as request_count,
                    AVG(response_time_ms) as avg_response_time,
                    SUM(response_size_bytes) as total_traffic_bytes,
                    MAX(time) as last_access
                FROM logs
                GROUP BY url
                ORDER BY request_count DESC
            """);

            // 2. Топ пользователей
            st.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_users AS
                SELECT 
                    username,
                    COUNT(*) as request_count,
                    COUNT(DISTINCT ip) as unique_ips,
                    AVG(response_time_ms) as avg_response_time,
                    SUM(response_size_bytes) as total_traffic_bytes
                FROM logs
                WHERE username != '-' AND username IS NOT NULL
                GROUP BY username
                ORDER BY request_count DESC
            """);

            // 3. Создаем индексы для быстрого обновления
            st.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_top_urls_url ON mv_top_urls(url)");
            st.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_top_users_username ON mv_top_users(username)");
        }
    }

    // Фоновая задача для обновления
    @Scheduled(fixedDelay = 300000) // Каждые 5 минут
    public void refreshTopStats() {
        jdbcTemplate.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_top_urls");
        jdbcTemplate.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_top_users");
    }
}