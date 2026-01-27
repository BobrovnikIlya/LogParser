package com.work.LogParser.service;

import com.work.LogParser.config.DatabaseConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;

@Service
public class LogDataRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private StatisticsService statisticsService;

    public Map<String, Object> getLogsWithStats(int page, int size,
                                                String dateFrom, String dateTo,
                                                String clientIp, String username,
                                                String status, String search, String action) {

        Map<String, Object> result = new HashMap<>();

        try {
            // Простая пагинация
            int offset = (page - 1) * size;

            // Строим базовый запрос
            StringBuilder where = new StringBuilder("WHERE 1=1");
            if (dateFrom != null && !dateFrom.isEmpty()) {
                where.append(" AND time >= '").append(dateFrom.replace("T", " ")).append("'");
            }
            if (dateTo != null && !dateTo.isEmpty()) {
                where.append(" AND time <= '").append(dateTo.replace("T", " ")).append("'");
            }
            if (clientIp != null && !clientIp.isEmpty()) {
                where.append(" AND ip = '").append(clientIp).append("'");
            }
            if (username != null && !username.isEmpty()) {
                where.append(" AND username = '").append(username).append("'");
            }
            if (status != null && !status.isEmpty()) {
                where.append(" AND status_code = ").append(status);
            }
            if (search != null && !search.isEmpty()) {
                where.append(" AND (url LIKE '%").append(search).append("%' OR domain LIKE '%").append(search).append("%')");
            }
            if (action != null && !action.isEmpty()) {
                where.append(" AND action = '").append(action).append("'");
            }
            // Получаем данные
            String sql = "SELECT " +
                    "id, " +
                    "time, " +
                    "ip, " +
                    "username, " +
                    "url, " +
                    "COALESCE(status_code, 0) as statusCode, " +           // statusCode с заменой NULL на 0
                    "domain, " +
                    "COALESCE(response_time_ms, 0) as responseTime, " +    // responseTime с заменой NULL на 0
                    "COALESCE(response_size_bytes, 0) as responseSize, " + // responseSize с заменой NULL на 0
                    "action " +
                    "FROM logs " + where + " ORDER BY time DESC LIMIT " + size + " OFFSET " + offset;

            System.out.println("SQL запрос для логов: " + sql);
            List<Map<String, Object>> logs = jdbcTemplate.queryForList(sql);

            for (Map<String, Object> log : logs) {
                // Приводим статус к Integer
                Object statusc = log.get("statusCode");
                if (statusc != null) {
                    try {
                        log.put("statusCode", ((Number) statusc).intValue());
                    } catch (Exception e) {
                        log.put("statusCode", 0);
                    }
                } else {
                    log.put("statusCode", 0);
                }

                // Приводим время ответа к Integer
                Object responseTime = log.get("responseTime");
                if (responseTime != null) {
                    try {
                        log.put("responseTime", ((Number) responseTime).intValue());
                    } catch (Exception e) {
                        log.put("responseTime", 0);
                    }
                } else {
                    log.put("responseTime", 0);
                }

                // Приводим размер ответа к Long
                Object responseSize = log.get("responseSize");
                if (responseSize != null) {
                    try {
                        log.put("responseSize", ((Number) responseSize).longValue());
                    } catch (Exception e) {
                        log.put("responseSize", 0L);
                    }
                } else {
                    log.put("responseSize", 0L);
                }
            }

            // Общее количество
            String countSql = "SELECT COUNT(*) FROM logs " + where;
            Long totalCount = jdbcTemplate.queryForObject(countSql, Long.class);
            int totalPages = (int) Math.ceil((double) (totalCount != null ? totalCount : 0) / size);

            // Базовая статистика
            Map<String, Object> stats = statisticsService.getBasicStats(where.toString());

            result.put("logs", logs);
            result.put("stats", stats);
            result.put("totalPages", totalPages);
            result.put("currentPage", page);

        } catch (Exception e) {
            // Логируем только если это не прерванное соединение
            if (!e.getMessage().contains("Программа на вашем хост-компьютере") &&
                    !e.getMessage().contains("Broken pipe")) {
                System.err.println("Ошибка получения логов: " + e.getMessage());
            }
            result.put("logs", new ArrayList<>());
            result.put("stats", statisticsService.getDefaultStats());
            result.put("totalPages", 1);
        }

        return result;
    }

    public boolean hasDataInDatabase() {
        try {
            // Сначала проверяем существование таблицы
            Boolean tableExists = jdbcTemplate.queryForObject(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'logs')",
                    Boolean.class
            );

            if (tableExists == null || !tableExists) {
                return false;
            }

            Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM logs LIMIT 1", Long.class);
            return count != null && count > 0;
        } catch (Exception e) {
            System.err.println("Ошибка проверки наличия данных: " + e.getMessage());
            return false;
        }
    }

    public long getLogCount() {
        try {
            Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM logs", Long.class);
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    public List<Map<String, Object>> getTopUrls(int limit) {
        List<Map<String, Object>> result = new ArrayList<>();
        String sql = "SELECT url, domain, COUNT(*) as count " +
                "FROM logs " +
                "WHERE url IS NOT NULL AND url != '' " +
                "GROUP BY url, domain " +
                "ORDER BY count DESC " +
                "LIMIT ?";

        try (Connection conn = DriverManager.getConnection(DatabaseConfig.DB_URL, DatabaseConfig.DB_USERNAME, DatabaseConfig.DB_PASSWORD);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setInt(1, limit);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("url", rs.getString("url"));
                    item.put("domain", rs.getString("domain"));
                    item.put("count", rs.getInt("count"));
                    result.add(item);
                }
            }

        } catch (SQLException e) {
            System.err.println("Ошибка получения топ URL: " + e.getMessage());
            e.printStackTrace();
        }

        return result;
    }

    public List<Map<String, Object>> getTopUsers(int limit) {
        List<Map<String, Object>> result = new ArrayList<>();

        String sql = "SELECT username, " +
                "ip, " +  // <-- ДОБАВЛЕНО: берем IP пользователя
                "COUNT(*) as count, " +
                "MIN(time) as first_seen, " +
                "MAX(time) as last_seen " +
                "FROM logs " +
                "WHERE username IS NOT NULL AND username != '' AND username != '-' " +
                "GROUP BY username, ip " +  // <-- ИЗМЕНЕНО: группируем по username и ip
                "ORDER BY count DESC " +
                "LIMIT ?";

        try (Connection conn = DriverManager.getConnection(DatabaseConfig.DB_URL, DatabaseConfig.DB_USERNAME, DatabaseConfig.DB_PASSWORD);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setInt(1, limit);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("username", rs.getString("username"));
                    item.put("ip", rs.getString("ip"));
                    item.put("count", rs.getInt("count"));
                    item.put("first_seen", rs.getTimestamp("first_seen"));
                    item.put("last_seen", rs.getTimestamp("last_seen"));
                    result.add(item);
                }
            }

        } catch (SQLException e) {
            System.err.println("Ошибка получения топ пользователей: " + e.getMessage());
            e.printStackTrace();
        }

        return result;
    }

    public List<Integer> getAvailableStatuses() {
        List<Integer> statuses = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(DatabaseConfig.DB_URL, DatabaseConfig.DB_USERNAME, DatabaseConfig.DB_PASSWORD)) {
            // Пробуем получить из таблицы статусов
            boolean hasStatusesTable = false;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'log_statuses')")) {
                if (rs.next()) {
                    hasStatusesTable = rs.getBoolean(1);
                }
            }

            if (hasStatusesTable) {
                String sql = "SELECT status_code FROM log_statuses ORDER BY status_code";
                try (PreparedStatement ps = conn.prepareStatement(sql);
                     ResultSet rs = ps.executeQuery()) {

                    while (rs.next()) {
                        statuses.add(rs.getInt(1));
                    }
                }
            } else {
                // Fallback: берем из logs
                String sql = "SELECT DISTINCT status_code FROM logs " +
                        "WHERE status_code IS NOT NULL AND status_code > 0 " +
                        "ORDER BY status_code ASC";
                try (PreparedStatement ps = conn.prepareStatement(sql);
                     ResultSet rs = ps.executeQuery()) {

                    while (rs.next()) {
                        statuses.add(rs.getInt(1));
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Ошибка получения списка статусов: " + e.getMessage());
        }

        return statuses;
    }

    public List<String> getAvailableActions() {
        List<String> actions = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(DatabaseConfig.DB_URL, DatabaseConfig.DB_USERNAME, DatabaseConfig.DB_PASSWORD)) {
            // Пробуем получить из таблицы actions
            boolean hasActionsTable = false;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'log_actions')")) {
                if (rs.next()) {
                    hasActionsTable = rs.getBoolean(1);
                }
            }

            String sql;
            if (hasActionsTable) {
                sql = "SELECT action FROM log_actions ORDER BY action";
            } else {
                sql = "SELECT DISTINCT action FROM logs " +
                        "WHERE action IS NOT NULL AND action != '' AND action != '-' " +
                        "ORDER BY action";
            }

            try (PreparedStatement ps = conn.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {
                    String action = rs.getString(1);
                    if (action != null && !action.trim().isEmpty()) {
                        actions.add(action.trim());
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Ошибка получения списка actions: " + e.getMessage());
        }

        return actions;
    }
}