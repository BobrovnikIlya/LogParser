package com.work.LogParser.repository;

import com.work.LogParser.config.DatabaseConfig;
import com.work.LogParser.service.StatisticsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;

import static com.work.LogParser.config.DatabaseConfig.*;

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

    private void buildWhereClause(StringBuilder where, List<Object> params,
                                  String dateFrom, String dateTo,
                                  String clientIp, String username,
                                  String status, String action) {

        List<String> conditions = new ArrayList<>();

        if (dateFrom != null && !dateFrom.isEmpty()) {
            conditions.add("time >= ?");
            params.add(dateFrom);
        }

        if (dateTo != null && !dateTo.isEmpty()) {
            conditions.add("time <= ?");
            params.add(dateTo);
        }

        if (clientIp != null && !clientIp.isEmpty()) {
            conditions.add("ip = ?");
            params.add(clientIp);
        }

        if (username != null && !username.isEmpty()) {
            conditions.add("username = ?");
            params.add(username);
        }

        if (status != null && !status.isEmpty()) {
            conditions.add("status_code = ?");
            params.add(Integer.parseInt(status));
        }

        if (action != null && !action.isEmpty()) {
            conditions.add("action = ?");
            params.add(action);
        }

        for (int i = 0; i < conditions.size(); i++) {
            if (i > 0) where.append(" AND ");
            where.append(conditions.get(i));
        }
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

    public List<Integer> getAvailableStatuses() {
        List<Integer> statuses = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {
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

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {
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

    public List<Map<String, Object>> getTopUrlsWithFilters(int limit,
                                                           String dateFrom, String dateTo,
                                                           String clientIp, String username,
                                                           String status, String action) {
        List<Map<String, Object>> result = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();

        // Формируем WHERE clause
        buildWhereClause(whereClause, params, dateFrom, dateTo, clientIp, username, status, action);

        String sql = "SELECT " +
                "url, " +
                "domain, " + // ДОБАВИТЬ domain в SELECT
                "COUNT(*)::BIGINT as request_count, " +
                "ROUND(AVG(response_time_ms))::INTEGER as avg_response_time, " +
                "SUM(response_size_bytes)::BIGINT as total_bytes, " +
                "MAX(time) as last_access " +
                "FROM logs " +
                "WHERE url IS NOT NULL AND url != '-' " + // ДОБАВИТЬ условия по url
                (whereClause.length() > 0 ? " AND " + whereClause.toString() : "") +
                " GROUP BY url, domain " + // ГРУППИРОВАТЬ по url и domain
                " ORDER BY request_count DESC " +
                " LIMIT ?";

        params.add(limit);

        try {
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
            for (Map<String, Object> row : rows) {
                Map<String, Object> item = new HashMap<>();
                item.put("url", row.get("url"));
                item.put("domain", row.get("domain")); // ДОБАВИТЬ domain

                // Исправляем приведение типов:
                Object countObj = row.get("request_count");
                item.put("count", countObj != null ? ((Number) countObj).longValue() : 0L);

                Object avgTimeObj = row.get("avg_response_time");
                item.put("avg_response_time", avgTimeObj != null ? ((Number) avgTimeObj).longValue() : 0L);

                Object bytesObj = row.get("total_bytes");
                Long bytes = bytesObj != null ? ((Number) bytesObj).longValue() : 0L;
                item.put("total_bytes", bytes);

                item.put("last_access", row.get("last_access"));

                // Конвертируем байты в МБ
                if (bytes != null && bytes > 0) {
                    item.put("total_mb", Math.round(bytes / (1024.0 * 1024.0) * 100.0) / 100.0);
                } else {
                    item.put("total_mb", 0.0);
                }

                result.add(item);
            }
        } catch (Exception e) {
            System.err.println("Ошибка при получении топ URL: " + e.getMessage());
            e.printStackTrace();
        }

        return result;
    }

    public List<Map<String, Object>> getTopUsersWithFilters(int limit,
                                                            String dateFrom, String dateTo,
                                                            String clientIp, String username,
                                                            String status, String action) {
        List<Map<String, Object>> result = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();

        // Формируем WHERE clause
        buildWhereClause(whereClause, params, dateFrom, dateTo, clientIp, username, status, action);

        String sql = "SELECT " +
                "username, " +
                "MIN(ip) as ip, " + // MIN(ip) вместо просто ip
                "COUNT(*)::BIGINT as request_count, " +
                "COUNT(DISTINCT ip)::BIGINT as unique_ips, " +
                "ROUND(AVG(response_time_ms))::INTEGER as avg_response_time, " +
                "SUM(response_size_bytes)::BIGINT as total_bytes, " +
                "MIN(time) as first_seen, " + // ДОБАВИТЬ
                "MAX(time) as last_seen " +   // ДОБАВИТЬ
                "FROM logs " +
                "WHERE username IS NOT NULL AND username != '-' " +
                (whereClause.length() > 0 ? " AND " + whereClause.toString() : "") +
                " GROUP BY username " + // ГРУППИРОВАТЬ только по username
                " ORDER BY request_count DESC " +
                " LIMIT ?";

        params.add(limit);

        try {
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
            for (Map<String, Object> row : rows) {
                Map<String, Object> item = new HashMap<>();
                item.put("username", row.get("username"));
                item.put("ip", row.get("ip"));

                // Исправляем приведение типов:
                Object countObj = row.get("request_count");
                item.put("count", countObj != null ? ((Number) countObj).longValue() : 0L);

                Object uniqueIpsObj = row.get("unique_ips");
                item.put("unique_ips", uniqueIpsObj != null ? ((Number) uniqueIpsObj).longValue() : 0L);

                Object avgTimeObj = row.get("avg_response_time");
                item.put("avg_response_time", avgTimeObj != null ? ((Number) avgTimeObj).longValue() : 0L);

                Object bytesObj = row.get("total_bytes");
                Long bytes = bytesObj != null ? ((Number) bytesObj).longValue() : 0L;
                item.put("total_bytes", bytes);

                item.put("first_seen", row.get("first_seen"));
                item.put("last_seen", row.get("last_seen"));

                // Конвертируем байты в МБ
                if (bytes != null && bytes > 0) {
                    item.put("total_mb", Math.round(bytes / (1024.0 * 1024.0) * 100.0) / 100.0);
                } else {
                    item.put("total_mb", 0.0);
                }

                result.add(item);
            }
        } catch (Exception e) {
            System.err.println("Ошибка при получении топ пользователей: " + e.getMessage());
            e.printStackTrace();
        }

        return result;
    }
}