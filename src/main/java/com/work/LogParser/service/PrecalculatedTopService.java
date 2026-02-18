package com.work.LogParser.service;

import com.work.LogParser.config.DatabaseConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

import static com.work.LogParser.config.DatabaseConfig.*;

@Service
public class PrecalculatedTopService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    // Получает прерассчитанные топ URL
    public List<Map<String, Object>> getPrecalculatedTopUrls(int limit) {
        return getPrecalculatedTops("urls", limit);
    }

    // Получает прерассчитанные топ пользователей
    public List<Map<String, Object>> getPrecalculatedTopUsers(int limit) {
        return getPrecalculatedTops("users", limit);
    }

    // Общая логика получения прерассчитанных топов
    private List<Map<String, Object>> getPrecalculatedTops(String type, int limit) {
        try (Connection conn = DriverManager.getConnection(
                DB_URL, DB_USERNAME, DB_PASSWORD)) {

            ensurePrecalculatedTopsTableExists(conn);

            String sql = "SELECT data_json, calculated_at " +
                    "FROM precalculated_tops " +
                    "WHERE type = ? AND limit_count >= ? " +
                    "ORDER BY limit_count ASC, calculated_at DESC " +
                    "LIMIT 1";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, type);
                ps.setInt(2, limit);

                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String json = rs.getString("data_json");
                        Timestamp calculatedAt = rs.getTimestamp("calculated_at");

                        if (json != null && !json.isEmpty()) {
                            List<Map<String, Object>> data = parseTopData(json);
                            // Обрезаем до нужного лимита
                            if (data.size() > limit) {
                                data = data.subList(0, limit);
                            }

                            System.out.printf("✅ Загружены прерассчитанные %s (лимит: %d, обновлено: %s)%n",
                                    type, limit, calculatedAt);
                            return data;
                        }
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("❌ Ошибка получения прерассчитанных топов: " + e.getMessage());
        }

        return Collections.emptyList();
    }

    // Обновляет все прерассчитанные топы
    public void updatePrecalculatedTops() {
        System.out.println("🔄 Обновление прерассчитанных топов...");

        try (Connection conn = DriverManager.getConnection(
                DB_URL, DB_USERNAME, DB_PASSWORD)) {

            ensurePrecalculatedTopsTableExists(conn);
            clearOldPrecalculatedTops(conn);

            // Топ URL - только 100
            List<Map<String, Object>> topUrls100 = calculateTopUrls(conn, 100);
            savePrecalculatedTop(conn, "urls", 100, topUrls100);
            System.out.println("  ✅ Топ URL (лимит: 100) сохранен");

            // Топ пользователей - только 10
            List<Map<String, Object>> topUsers10 = calculateTopUsers(conn, 10);
            savePrecalculatedTop(conn, "users", 10, topUsers10);
            System.out.println("  ✅ Топ пользователей (лимит: 10) сохранен");

            System.out.println("✅ Все прерассчитанные топы обновлены");

        } catch (Exception e) {
            System.err.println("❌ Ошибка обновления прерассчитанных топов: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Рассчитывает топ URL
    private List<Map<String, Object>> calculateTopUrls(Connection conn, int limit) throws SQLException {
        String sql = "SELECT " +
                "url, " +
                "domain, " +
                "COUNT(*) as request_count, " +
                "ROUND(AVG(response_time_ms)) as avg_response_time, " +
                "SUM(response_size_bytes) as total_bytes, " +
                "MAX(time) as last_access " +
                "FROM logs " +
                "WHERE url IS NOT NULL AND url != '-' " +
                "GROUP BY url, domain " +
                "ORDER BY request_count DESC " +
                "LIMIT ?";

        List<Map<String, Object>> result = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, limit);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("url", rs.getString("url"));
                    item.put("domain", rs.getString("domain"));
                    item.put("count", rs.getLong("request_count"));
                    item.put("avg_response_time", rs.getLong("avg_response_time"));
                    item.put("total_bytes", rs.getLong("total_bytes"));
                    item.put("last_access", rs.getTimestamp("last_access"));

                    // Конвертируем байты в МБ
                    Object bytesObj = rs.getObject("total_bytes");
                    Long bytes = bytesObj != null ?
                            ((Number) bytesObj).longValue() : null;
                    if (bytes != null) {
                        item.put("total_mb", Math.round(bytes / (1024.0 * 1024.0) * 100.0) / 100.0);
                    }

                    result.add(item);
                }
            }
        }

        return result;
    }

    // Рассчитывает топ пользователей
    private List<Map<String, Object>> calculateTopUsers(Connection conn, int limit) throws SQLException {
        String sql = "SELECT " +
                "min(ip) as ip," +
                "username, " +
                "COUNT(*) as request_count, " +
                "COUNT(DISTINCT ip) as unique_ips, " +
                "ROUND(AVG(response_time_ms)) as avg_response_time, " +
                "SUM(response_size_bytes) as total_bytes, " +
                "MIN(time) as first_seen, " +
                "MAX(time) as last_seen " +
                "FROM logs " +
                "WHERE username IS NOT NULL AND username != '-' " +
                "GROUP BY username " +
                "ORDER BY request_count DESC " +
                "LIMIT ?";

        List<Map<String, Object>> result = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, limit);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("username", rs.getString("username"));
                    item.put("ip", rs.getString("ip"));
                    item.put("first_seen", rs.getTimestamp("first_seen"));
                    item.put("last_seen", rs.getTimestamp("last_seen"));
                    item.put("count", rs.getLong("request_count"));
                    item.put("unique_ips", rs.getLong("unique_ips"));
                    item.put("avg_response_time", rs.getLong("avg_response_time"));
                    item.put("total_bytes", rs.getLong("total_bytes"));

                    // Конвертируем байты в МБ
                    Object bytesObj = rs.getObject("total_bytes");
                    Long bytes = bytesObj != null ?
                            ((Number) bytesObj).longValue() : null;
                    if (bytes != null) {
                        item.put("total_mb", Math.round(bytes / (1024.0 * 1024.0) * 100.0) / 100.0);
                    }

                    result.add(item);
                }
            }
        }

        return result;
    }

    // Сохраняет прерассчитанный топ в БД
    private void savePrecalculatedTop(Connection conn, String type, int limit,
                                      List<Map<String, Object>> data) throws SQLException, JsonProcessingException {

        String jsonData = objectMapper.writeValueAsString(data);

        String sql = "INSERT INTO precalculated_tops (type, limit_count, data_json, calculated_at) " +
                "VALUES (?, ?, ?, ?) " +
                "ON CONFLICT (type, limit_count) DO UPDATE SET " +
                "data_json = EXCLUDED.data_json, " +
                "calculated_at = EXCLUDED.calculated_at";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, type);
            ps.setInt(2, limit);
            ps.setString(3, jsonData);
            ps.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));

            ps.executeUpdate();
        }
    }

    // Создает таблицу для прерассчитанных топов если не существует
    private void ensurePrecalculatedTopsTableExists(Connection conn) throws SQLException {
        if (tableExists(conn, "precalculated_tops")) {
            return;
        }

        String createTableSQL = "CREATE TABLE precalculated_tops (" +
                "id BIGSERIAL PRIMARY KEY," +
                "type VARCHAR(50) NOT NULL," +  // 'urls' или 'users'
                "limit_count INT NOT NULL," +    // лимит (10, 50, 100 и т.д.)
                "data_json TEXT NOT NULL," +     // JSON с данными
                "calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                "UNIQUE(type, limit_count)" +    // уникальная комбинация
                ")";

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);
            System.out.println("✅ Таблица precalculated_tops создана");

            // Индексы для быстрого поиска
            stmt.execute("CREATE INDEX idx_precalculated_tops_type_limit ON precalculated_tops(type, limit_count)");
            stmt.execute("CREATE INDEX idx_precalculated_tops_calculated ON precalculated_tops(calculated_at DESC)");
        }
    }

    // Проверяет существование таблицы
    private boolean tableExists(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    // Очищает старые прерассчитанные данные (оставляет только последние для каждой комбинации)
    private void clearOldPrecalculatedTops(Connection conn) throws SQLException {
        // Удаляем дубликаты, оставляя только последние записи для каждой type+limit
        String sql = "DELETE FROM precalculated_tops WHERE id NOT IN (" +
                "SELECT DISTINCT ON (type, limit_count) id " +
                "FROM precalculated_tops " +
                "ORDER BY type, limit_count, calculated_at DESC" +
                ")";

        try (Statement stmt = conn.createStatement()) {
            int deleted = stmt.executeUpdate(sql);
            if (deleted > 0) {
                System.out.println("🗑️ Удалено старых прерассчитанных топов: " + deleted);
            }
        }
    }

    // Парсит JSON данные
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> parseTopData(String json) throws JsonProcessingException {
        return objectMapper.readValue(json, List.class);
    }
}