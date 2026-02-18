package com.work.LogParser.service;

import com.work.LogParser.config.DatabaseConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.work.LogParser.config.DatabaseConfig.*;

@Service
public class AggregatedStatsService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    // Сохраняет агрегированную статистику в БД
    public void saveAggregatedStats(Map<String, Object> stats,
                                    LocalDateTime periodStart,
                                    LocalDateTime periodEnd,
                                    boolean isDefault) {
        try (Connection conn = DriverManager.getConnection(
                DB_URL, DB_USERNAME, DB_PASSWORD)) {

            ensureStatsTableExists(conn);

            if (isDefault) {
                clearDefaultStats(conn);
            }

            // Конвертируем данные в JSON
            String statusDistributionJson = convertMapToJson((Map<String, Integer>) stats.get("status_distribution"));
            String hourlyDistributionJson = convertArrayToJson((int[]) stats.get("hourly_distribution"));

            // Получаем и сохраняем топы
            String topUrlsJson = getTopUrlsAsJson(conn, periodStart, periodEnd, 100);
            String topUsersJson = getTopUsersAsJson(conn, periodStart, periodEnd, 10);

            // Сохраняем в БД
            String sql = "INSERT INTO aggregated_stats (" +
                    "period_start, period_end, " +
                    "total_requests, error_requests, unique_ips, " +
                    "avg_response_time, total_traffic_mb, " +
                    "status_distribution_json, hourly_distribution_json, " +
                    "top_urls_json, top_users_json, " +
                    "is_default, created_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setTimestamp(1, periodStart != null ? Timestamp.valueOf(periodStart) : null);
                ps.setTimestamp(2, periodEnd != null ? Timestamp.valueOf(periodEnd) : null);
                ps.setLong(3, (Long) stats.getOrDefault("total_requests", 0L));
                ps.setLong(4, (Long) stats.getOrDefault("error_requests", 0L));
                ps.setLong(5, (Long) stats.getOrDefault("unique_ips", 0L));
                ps.setDouble(6, ((Number) stats.getOrDefault("avg_response_time", 0)).doubleValue());
                ps.setDouble(7, ((Number) stats.getOrDefault("total_traffic_mb", 0)).doubleValue());
                ps.setString(8, statusDistributionJson);
                ps.setString(9, hourlyDistributionJson);
                ps.setString(10, topUrlsJson);
                ps.setString(11, topUsersJson);
                ps.setBoolean(12, isDefault);
                ps.setTimestamp(13, Timestamp.valueOf(LocalDateTime.now()));

                ps.executeUpdate();
                System.out.println("✅ Агрегированная статистика с топами сохранена (is_default: " + isDefault + ")");
            }

        } catch (Exception e) {
            System.err.println("❌ Ошибка сохранения агрегированной статистики: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Получает агрегированную статистику из БД по периоду
    public Map<String, Object> getAggregatedStats(LocalDateTime dateFrom, LocalDateTime dateTo) {
        try (Connection conn = DriverManager.getConnection(
                DB_URL, DB_USERNAME, DB_PASSWORD)) {

            if (!statsTableExists(conn)) {
                return null;
            }

            String sql;
            PreparedStatement ps;

            if (dateFrom == null && dateTo == null) {
                // Получаем статистику по всем данным (дефолтную)
                sql = "SELECT * FROM aggregated_stats WHERE is_default = true ORDER BY created_at DESC LIMIT 1";
                ps = conn.prepareStatement(sql);
            } else {
                // Получаем статистику за период
                sql = "SELECT * FROM aggregated_stats " +
                        "WHERE (? IS NULL OR period_end >= ?) " +
                        "AND (? IS NULL OR period_start <= ?) " +
                        "AND is_default = false " +
                        "ORDER BY created_at DESC LIMIT 1";
                ps = conn.prepareStatement(sql);
                ps.setTimestamp(1, dateFrom != null ? Timestamp.valueOf(dateFrom) : null);
                ps.setTimestamp(2, dateFrom != null ? Timestamp.valueOf(dateFrom) : null);
                ps.setTimestamp(3, dateTo != null ? Timestamp.valueOf(dateTo) : null);
                ps.setTimestamp(4, dateTo != null ? Timestamp.valueOf(dateTo) : null);
            }

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return convertResultSetToStats(rs);
                }
            }

        } catch (Exception e) {
            System.err.println("❌ Ошибка получения агрегированной статистики: " + e.getMessage());
        }

        return null;
    }

    // Проверяет существование таблицы
    private boolean statsTableExists(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'aggregated_stats')")) {
            return rs.next() && rs.getBoolean(1);
        }
    }

    // Создает таблицу для агрегированной статистики
    private void ensureStatsTableExists(Connection conn) throws SQLException {
        if (statsTableExists(conn)) {
            // Проверяем есть ли колонки для топов, если нет - добавляем
            if (!columnExists(conn, "aggregated_stats", "top_urls_json")) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("ALTER TABLE aggregated_stats ADD COLUMN top_urls_json TEXT");
                    stmt.execute("ALTER TABLE aggregated_stats ADD COLUMN top_users_json TEXT");
                    System.out.println("✅ Добавлены колонки для топов в aggregated_stats");
                }
            }
            return;
        }

        String createTableSQL = "CREATE TABLE aggregated_stats (" +
                "id BIGSERIAL PRIMARY KEY," +
                "period_start TIMESTAMP," +
                "period_end TIMESTAMP," +
                "total_requests BIGINT NOT NULL DEFAULT 0," +
                "error_requests BIGINT NOT NULL DEFAULT 0," +
                "unique_ips BIGINT NOT NULL DEFAULT 0," +
                "avg_response_time DOUBLE PRECISION NOT NULL DEFAULT 0," +
                "total_traffic_mb DOUBLE PRECISION NOT NULL DEFAULT 0," +
                "status_distribution_json TEXT," +
                "hourly_distribution_json TEXT," +
                "top_urls_json TEXT," +           // Новое поле для топ URL
                "top_users_json TEXT," +          // Новое поле для топ пользователей
                "is_default BOOLEAN NOT NULL DEFAULT false," +
                "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP" +
                ")";

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);
            System.out.println("✅ Таблица aggregated_stats создана с полями для топов");

            // Создаем индексы
            stmt.execute("CREATE INDEX idx_aggregated_stats_period ON aggregated_stats(period_start, period_end)");
            stmt.execute("CREATE INDEX idx_aggregated_stats_default ON aggregated_stats(is_default)");
            stmt.execute("CREATE INDEX idx_aggregated_stats_created ON aggregated_stats(created_at)");
        }
    }

    private boolean columnExists(Connection conn, String tableName, String columnName) throws SQLException {
        String sql = "SELECT EXISTS (SELECT FROM information_schema.columns " +
                "WHERE table_name = ? AND column_name = ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableName);
            ps.setString(2, columnName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    // Очищает старую дефолтную статистику
    private void clearDefaultStats(Connection conn) throws SQLException {
        String sql = "DELETE FROM aggregated_stats WHERE is_default = true";
        try (Statement stmt = conn.createStatement()) {
            int deleted = stmt.executeUpdate(sql);
            if (deleted > 0) {
                System.out.println("🗑️ Удалено старых дефолтных статистик: " + deleted);
            }
        }
    }

    // Конвертирует ResultSet в Map статистики
    private Map<String, Object> convertResultSetToStats(ResultSet rs) throws SQLException, JsonProcessingException {
        Map<String, Object> stats = new HashMap<>();

        stats.put("total_requests", rs.getLong("total_requests"));
        stats.put("error_requests", rs.getLong("error_requests"));
        stats.put("avg_response_time", rs.getDouble("avg_response_time"));
        stats.put("unique_ips", rs.getLong("unique_ips"));
        stats.put("total_traffic_mb", rs.getDouble("total_traffic_mb"));

        // Парсим JSON данные
        String statusJson = rs.getString("status_distribution_json");
        String hourlyJson = rs.getString("hourly_distribution_json");

        if (statusJson != null && !statusJson.isEmpty()) {
            stats.put("status_distribution", objectMapper.readValue(statusJson, Map.class));
        } else {
            stats.put("status_distribution", new HashMap<>());
        }

        if (hourlyJson != null && !hourlyJson.isEmpty()) {
            stats.put("hourly_distribution", objectMapper.readValue(hourlyJson, int[].class));
        } else {
            stats.put("hourly_distribution", new int[24]);
        }

        return stats;
    }

    // Конвертирует Map в JSON строку
    private String convertMapToJson(Map<String, Integer> map) throws JsonProcessingException {
        return objectMapper.writeValueAsString(map != null ? map : new HashMap<>());
    }

    // Конвертирует массив в JSON строку
    private String convertArrayToJson(int[] array) throws JsonProcessingException {
        return objectMapper.writeValueAsString(array != null ? array : new int[24]);
    }

    // Вычисляет и сохраняет статистику по всем данным
    public void calculateAndSaveDefaultStats() {
        try (Connection conn = DriverManager.getConnection(
                DB_URL, DB_USERNAME, DB_PASSWORD)) {

            System.out.println("📊 Вычисление дефолтной статистики по всем данным...");

            // Получаем временные границы
            LocalDateTime minDate = null;
            LocalDateTime maxDate = null;

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT MIN(time), MAX(time) FROM logs")) {
                if (rs.next()) {
                    Timestamp minTs = rs.getTimestamp(1);
                    Timestamp maxTs = rs.getTimestamp(2);
                    if (minTs != null) minDate = minTs.toLocalDateTime();
                    if (maxTs != null) maxDate = maxTs.toLocalDateTime();
                }
            }

            // Вычисляем статистику
            Map<String, Object> stats = calculateStatsForPeriod(null, null, conn);

            // Сохраняем
            if (stats != null && !stats.isEmpty()) {
                saveAggregatedStats(stats, minDate, maxDate, true);
                System.out.println("✅ Дефолтная статистика сохранена");
            }

        } catch (Exception e) {
            System.err.println("❌ Ошибка вычисления дефолтной статистики: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Вычисляет статистику для периода
    private Map<String, Object> calculateStatsForPeriod(LocalDateTime dateFrom, LocalDateTime dateTo, Connection conn)
            throws SQLException {

        StringBuilder where = new StringBuilder("WHERE 1=1");

        if (dateFrom != null) {
            where.append(" AND time >= '").append(dateFrom.toString()).append("'");
        }
        if (dateTo != null) {
            where.append(" AND time <= '").append(dateTo.toString()).append("'");
        }

        Map<String, Object> stats = new HashMap<>();

        try (Statement stmt = conn.createStatement()) {
            // 1. Общее количество запросов
            String countSql = "SELECT COUNT(*) FROM logs " + where;
            Long totalRequests = executeCountQuery(countSql, stmt);
            stats.put("total_requests", totalRequests != null ? totalRequests : 0L);

            // 2. Ошибочные запросы
            Long errorRequests = executeCountQuery(
                    "SELECT COUNT(*) FROM logs " + where +
                            " AND status_code >= 400", stmt);
            stats.put("error_requests", errorRequests != null ? errorRequests : 0);

            // 3. Уникальные IP
            Long uniqueIps = executeCountQuery("SELECT COUNT(DISTINCT ip) FROM logs " + where, stmt);
            stats.put("unique_ips", uniqueIps != null ? uniqueIps : 0);

            // 4. Распределение HTTP статусов
            Map<String, Integer> statusDistribution = getStatusDistribution(where.toString(), stmt);
            stats.put("status_distribution", statusDistribution);

            // 5. Распределение по часам
            int[] hourlyDistribution = getHourlyDistribution(where.toString(), stmt);
            stats.put("hourly_distribution", hourlyDistribution);

            // 6. Среднее время ответа
            Double avgResponseTime = executeDoubleQuery(
                    "SELECT AVG(response_time_ms) FROM logs " + where +
                            " AND response_time_ms > 0", stmt);
            stats.put("avg_response_time", avgResponseTime != null ? Math.round(avgResponseTime) : 0);

            // 7. Общий трафик в МБ
            Long totalTrafficBytes = executeLongQuery(
                    "SELECT COALESCE(SUM(response_size_bytes), 0) FROM logs " + where, stmt);
            double totalTrafficMB = totalTrafficBytes != null ?
                    totalTrafficBytes / (1024.0 * 1024.0) : 0;
            stats.put("total_traffic_mb", Math.round(totalTrafficMB * 100.0) / 100.0);
        }

        return stats;
    }

    // Вспомогательные методы для выполнения запросов
    private Long executeCountQuery(String sql, Statement stmt) throws SQLException {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    private Double executeDoubleQuery(String sql, Statement stmt) throws SQLException {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getDouble(1) : 0.0;
        }
    }

    private Long executeLongQuery(String sql, Statement stmt) throws SQLException {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    private Map<String, Integer> getStatusDistribution(String where, Statement stmt) throws SQLException {
        Map<String, Integer> distribution = new HashMap<>();

        String statusQuery = "SELECT " +
                "CASE " +
                "  WHEN status_code >= 200 AND status_code < 300 THEN '2xx (Успех)' " +
                "  WHEN status_code >= 300 AND status_code < 400 THEN '3xx (Перенаправление)' " +
                "  WHEN status_code >= 400 AND status_code < 500 THEN '4xx (Ошибка клиента)' " +
                "  WHEN status_code >= 500 THEN '5xx (Ошибка сервера)' " +
                "  ELSE 'Другие' " +
                "END as status_group, " +
                "COUNT(*) as count " +
                "FROM logs " + where +
                " GROUP BY status_group " +
                " ORDER BY count DESC";

        try (ResultSet rs = stmt.executeQuery(statusQuery)) {
            while (rs.next()) {
                distribution.put(rs.getString("status_group"), rs.getInt("count"));
            }
        }

        return distribution;
    }

    private int[] getHourlyDistribution(String where, Statement stmt) throws SQLException {
        int[] hourlyDistribution = new int[24];

        String hourlyQuery = "SELECT EXTRACT(HOUR FROM time) as hour, COUNT(*) as count " +
                "FROM logs " + where +
                " GROUP BY EXTRACT(HOUR FROM time) ORDER BY hour";

        try (ResultSet rs = stmt.executeQuery(hourlyQuery)) {
            while (rs.next()) {
                int hour = rs.getInt("hour");
                int count = rs.getInt("count");
                if (hour >= 0 && hour < 24) {
                    hourlyDistribution[hour] = count;
                }
            }
        }

        return hourlyDistribution;
    }

    private String getTopUrlsAsJson(Connection conn, LocalDateTime dateFrom,
                                    LocalDateTime dateTo, int limit) throws SQLException, JsonProcessingException {

        StringBuilder where = new StringBuilder();
        List<Object> params = new ArrayList<>();

        if (dateFrom != null) {
            where.append(" AND time >= ?");
            params.add(Timestamp.valueOf(dateFrom)); // ✅ Используем Timestamp
        }
        if (dateTo != null) {
            where.append(" AND time <= ?");
            params.add(Timestamp.valueOf(dateTo)); // ✅ Используем Timestamp
        }

        String sql = "SELECT url, COUNT(*) as request_count " +
                "FROM logs " +
                "WHERE url IS NOT NULL AND url != '-' " +
                where.toString() +
                " GROUP BY url " +
                " ORDER BY request_count DESC " +
                " LIMIT ?";

        params.add(limit);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            // Устанавливаем параметры с правильными типами
            for (int i = 0; i < params.size(); i++) {
                Object param = params.get(i);
                if (param instanceof Timestamp) {
                    ps.setTimestamp(i + 1, (Timestamp) param);
                } else if (param instanceof Integer) {
                    ps.setInt(i + 1, (Integer) param);
                } else {
                    ps.setObject(i + 1, param);
                }
            }

            List<Map<String, Object>> results = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("url", rs.getString("url"));
                    Object countObj = rs.getObject("request_count");
                    item.put("count", countObj != null ?
                            ((Number) countObj).longValue() : 0L);
                    results.add(item);
                }
            }

            return objectMapper.writeValueAsString(results);
        }
    }

    private String getTopUsersAsJson(Connection conn, LocalDateTime dateFrom,
                                     LocalDateTime dateTo, int limit) throws SQLException, JsonProcessingException {

        StringBuilder where = new StringBuilder();
        List<Object> params = new ArrayList<>();

        if (dateFrom != null) {
            where.append(" AND time >= ?");
            params.add(Timestamp.valueOf(dateFrom)); // ✅ Используем Timestamp
        }
        if (dateTo != null) {
            where.append(" AND time <= ?");
            params.add(Timestamp.valueOf(dateTo)); // ✅ Используем Timestamp
        }

        String sql = "SELECT username, COUNT(*) as request_count " +
                "FROM logs " +
                "WHERE username IS NOT NULL AND username != '-' " +
                where.toString() +
                " GROUP BY username " +
                " ORDER BY request_count DESC " +
                " LIMIT ?";

        params.add(limit);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            // Устанавливаем параметры с правильными типами
            for (int i = 0; i < params.size(); i++) {
                Object param = params.get(i);
                if (param instanceof Timestamp) {
                    ps.setTimestamp(i + 1, (Timestamp) param);
                } else if (param instanceof Integer) {
                    ps.setInt(i + 1, (Integer) param);
                } else {
                    ps.setObject(i + 1, param);
                }
            }

            List<Map<String, Object>> results = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("username", rs.getString("username"));
                    Object countObj = rs.getObject("request_count");
                    item.put("count", countObj != null ?
                            ((Number) countObj).longValue() : 0L);
                    results.add(item);
                }
            }

            return objectMapper.writeValueAsString(results);
        }
    }
}