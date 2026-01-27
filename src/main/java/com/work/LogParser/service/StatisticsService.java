package com.work.LogParser.service;

import com.work.LogParser.config.DatabaseConfig;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

@Service
public class StatisticsService {

    public Map<String, Object> getBasicStats(String whereClause) {
        Map<String, Object> stats = new HashMap<>();

        try {
            // Базовый WHERE (уже приходит сформированным)
            String baseQuery = "FROM logs " + (whereClause.isEmpty() ? "" : whereClause);

            // 1. Общее количество запросов
            Long totalRequests = executeCountQuery("SELECT COUNT(*) " + baseQuery);
            stats.put("total_requests", totalRequests != null ? totalRequests : 0);

            // 2. Ошибочные запросы
            Long errorRequests = executeCountQuery(
                    "SELECT COUNT(*) FROM logs " +
                            (whereClause.isEmpty() ? "WHERE" : whereClause + " AND") +
                            " status_code >= 400"
            );
            stats.put("error_requests", errorRequests != null ? errorRequests : 0);

            // 3. Уникальные IP
            Long uniqueIps = executeCountQuery("SELECT COUNT(DISTINCT ip) " + baseQuery);
            stats.put("unique_ips", uniqueIps != null ? uniqueIps : 0);

            // 4. Распределение HTTP статусов (группируем по классам)
            String statusQuery = "SELECT " +
                    "CASE " +
                    "  WHEN status_code >= 200 AND status_code < 300 THEN '2xx (Успех)' " +
                    "  WHEN status_code >= 300 AND status_code < 400 THEN '3xx (Перенаправление)' " +
                    "  WHEN status_code >= 400 AND status_code < 500 THEN '4xx (Ошибка клиента)' " +
                    "  WHEN status_code >= 500 THEN '5xx (Ошибка сервера)' " +
                    "  ELSE 'Другие' " +
                    "END as status_group, " +
                    "COUNT(*) as count " +
                    "FROM logs " + (whereClause.isEmpty() ? "" : whereClause) +
                    " GROUP BY status_group " +
                    " ORDER BY count DESC";

            Map<String, Integer> statusDistribution = new HashMap<>();
            try (Connection conn = DriverManager.getConnection(DatabaseConfig.DB_URL, DatabaseConfig.DB_USERNAME, DatabaseConfig.DB_PASSWORD);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(statusQuery)) {

                while (rs.next()) {
                    String statusGroup = rs.getString("status_group");
                    int count = rs.getInt("count");
                    statusDistribution.put(statusGroup, count);
                }
            }
            stats.put("status_distribution", statusDistribution);

            // 5. Распределение по часам
            String hourlyQuery = "SELECT EXTRACT(HOUR FROM time) as hour, COUNT(*) as count " +
                    "FROM logs " + (whereClause.isEmpty() ? "" : whereClause) +
                    " GROUP BY EXTRACT(HOUR FROM time) ORDER BY hour";

            int[] hourlyDistribution = new int[24];
            try (Connection conn = DriverManager.getConnection(DatabaseConfig.DB_URL, DatabaseConfig.DB_USERNAME, DatabaseConfig.DB_PASSWORD);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(hourlyQuery)) {

                while (rs.next()) {
                    int hour = rs.getInt("hour");
                    int count = rs.getInt("count");
                    if (hour >= 0 && hour < 24) {
                        hourlyDistribution[hour] = count;
                    }
                }
            }
            stats.put("hourly_distribution", hourlyDistribution);

            // 6. Заглушки для совместимости
            stats.put("avg_response_time", 0);
            stats.put("total_traffic_mb", 0);

            // 7. Среднее время ответа (только для записей с response_time_ms > 0)
            String avgResponseTimeQuery = "SELECT AVG(response_time_ms) " + baseQuery +
                    " AND response_time_ms > 0";
            Double avgResponseTime = executeDoubleQuery(avgResponseTimeQuery);
            stats.put("avg_response_time",
                    avgResponseTime != null ? Math.round(avgResponseTime) : 0);

            // 8. Общий трафик в МБ
            String totalTrafficQuery = "SELECT COALESCE(SUM(response_size_bytes), 0) " + baseQuery;
            Long totalTrafficBytes = executeLongQuery(totalTrafficQuery);
            double totalTrafficMB = totalTrafficBytes != null ?
                    totalTrafficBytes / (1024.0 * 1024.0) : 0;
            stats.put("total_traffic_mb", Math.round(totalTrafficMB * 100.0) / 100.0);

        } catch (Exception e) {
            System.err.println("Ошибка получения статистики: " + e.getMessage());
            stats = getDefaultStats();
        }

        return stats;
    }

    public Double executeDoubleQuery(String sql) {
        try (Connection conn = DriverManager.getConnection(DatabaseConfig.DB_URL, DatabaseConfig.DB_USERNAME, DatabaseConfig.DB_PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                return rs.getDouble(1);
            }
            return 0.0;
        } catch (Exception e) {
            System.err.println("Ошибка выполнения AVG запроса: " + e.getMessage());
            return 0.0;
        }
    }

    public Long executeLongQuery(String sql) {
        try (Connection conn = DriverManager.getConnection(DatabaseConfig.DB_URL, DatabaseConfig.DB_USERNAME, DatabaseConfig.DB_PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0L;
        } catch (Exception e) {
            System.err.println("Ошибка выполнения SUM запроса: " + e.getMessage());
            return 0L;
        }
    }

    public Long executeCountQuery(String sql) {
        try (Connection conn = DriverManager.getConnection(DatabaseConfig.DB_URL, DatabaseConfig.DB_USERNAME, DatabaseConfig.DB_PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0L;
        } catch (Exception e) {
            System.err.println("Ошибка выполнения COUNT запроса: " + e.getMessage());
            return 0L;
        }
    }

    public Map<String, Object> getDefaultStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("total_requests", 0);
        stats.put("error_requests", 0);
        stats.put("avg_response_time", 0);
        stats.put("unique_ips", 0);
        stats.put("total_traffic_mb", 0);
        stats.put("status_distribution", new HashMap<>());
        stats.put("hourly_distribution", new int[24]);
        return stats;
    }
}