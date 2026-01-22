package com.work.LogParser.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

@Service
public class LogParsingService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile ParsingStatus currentStatus = new ParsingStatus();
    private Future<?> parsingTask;

    // Статические поля из вашего DataLoader
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/ParserLog";
    private static final String DB_USERNAME = "postgres";
    private static final String DB_PASSWORD = "uthgb123";

    private static final Pattern LOG_PATTERN = Pattern.compile(
            "^(\\d+\\.\\d+)\\s+" +
                    "\\d+\\s+" +
                    "(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+" +
                    "(\\S+)/(\\d+)\\s+" +
                    "\\d+\\s+" +
                    "\\S+\\s+" +
                    "(\\S+)\\s+" +
                    "(\\S+|-)"
    );

    private static final Pattern USERNAME_PATTERN = Pattern.compile(
            "^(.*_.*_.*|.*user.*)$", Pattern.CASE_INSENSITIVE
    );

    private static final Pattern DOMAIN_PATTERN = Pattern.compile(
            "^(?:https?://)?([^/:]+)(?::\\d+)?(?:/.*)?$"
    );

    // Простой класс для статуса
    private static class ParsingStatus {
        boolean isParsing = false;
        String status = "Готов к работе";
        double progress = 0;
        long processed = 0;
        long total = 0;
        String filePath;
        long startTime;
    }

    public boolean startParsing(String filePath) {
        System.out.println("Сервис: попытка запуска парсинга для файла: " + filePath);

        if (currentStatus.isParsing) {
            System.out.println("Сервис: парсинг уже выполняется, отказ");
            return false;
        }

        // Сбрасываем статус
        currentStatus = new ParsingStatus();
        currentStatus.isParsing = true;
        currentStatus.status = "Начало парсинга";
        currentStatus.filePath = filePath;
        currentStatus.startTime = System.currentTimeMillis();

        System.out.println("Сервис: запуск парсинга в отдельном потоке");

        // Запускаем парсинг в отдельном потоке
        parsingTask = executor.submit(() -> {
            try {
                parseWithOriginalCode(filePath);
            } catch (Exception e) {
                System.err.println("Сервис: ошибка в потоке парсинга: " + e.getMessage());
                e.printStackTrace();
                currentStatus.isParsing = false;
                currentStatus.status = "❌ Ошибка: " + e.getMessage();
            }
        });

        return true;
    }

    private void parseWithOriginalCode(String filePath) {
        // ВАШ ОРИГИНАЛЬНЫЙ КОД ИЗ DataLoader с небольшими изменениями для прогресса
        int count = 0;
        int batchSize = 5000;
        int skipped = 0;
        int filteredByUsername = 0;
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {

            // 1. Проверяем нужно ли парсить
            if (!shouldParseLogs(conn, filePath)) {
                currentStatus.status = "Парсинг не требуется - данные актуальны";
                currentStatus.progress = 100;
                currentStatus.isParsing = false;
                return;
            }
            // 2. Считаем строки для прогресса (опционально)
            currentStatus.status = "Подсчет строк...";
            long totalLines = countLines(filePath);
            currentStatus.total = totalLines;

            // 3. Очищаем таблицу
            currentStatus.status = "Очистка таблицы...";
            clearLogsTable(conn);

            // 4. Создаем таблицу и индексы (UNLOGGED для скорости)
            currentStatus.status = "Создание таблиц...";
            createUnloggedTable(conn);

            // 5. Парсим файл
            currentStatus.status = "Парсинг файла...";
            try (BufferedReader br = new BufferedReader(new java.io.FileReader(filePath));
                 PreparedStatement ps = conn.prepareStatement(
                         "INSERT INTO logs_unlogged (time, ip, username, url, status_code, domain) VALUES (?, ?, ?, ?, ?, ?)")) {

                String line;
                long processed = 0;

                while ((line = br.readLine()) != null) {
                    processed++;

                    try {
                        java.util.regex.Matcher m = LOG_PATTERN.matcher(line);
                        if (m.find()) {
                            String rawTime = m.group(1);
                            String ip = m.group(2);
                            int statusCode = Integer.parseInt(m.group(4));
                            String url = m.group(5);
                            String username = m.group(6);

                            if (username != null) {
                                username = username.trim();
                                if (!username.isEmpty() && !username.equals("-")) {
                                    if (isValidUsername(username)) {
                                        LocalDateTime dateTime = convertTimestamp(rawTime);
                                        if (dateTime != null) {
                                            String domain = extractDomain(url);

                                            ps.setObject(1, dateTime);
                                            ps.setString(2, ip);
                                            ps.setString(3, username);
                                            ps.setString(4, url);
                                            ps.setInt(5, statusCode);
                                            ps.setString(6, domain);
                                            ps.addBatch();
                                            count++;

                                            if (count % batchSize == 0) {
                                                ps.executeBatch();
                                                // Обновляем прогресс
                                                currentStatus.processed = processed;
                                                currentStatus.progress = (processed * 100.0) / totalLines;
                                                currentStatus.status = String.format("Обработано: %,d строк", processed);
                                            }
                                        }
                                    } else {
                                        filteredByUsername++;
                                    }
                                }
                            }
                        } else {
                            skipped++;
                        }
                    } catch (Exception e) {
                        skipped++;
                    }

                    // Обновляем прогресс каждые 10000 строк
                    if (processed % 10000 == 0) {
                        currentStatus.processed = processed;
                        currentStatus.progress = (processed * 100.0) / totalLines;
                        currentStatus.status = String.format("Обработано: %,d из %,d строк (%.1f%%)",
                                processed, totalLines, currentStatus.progress);
                    }
                    // Выводим в лог каждые 500к строк
                    if (processed % 500000 == 0) {
                        System.out.println(currentStatus.status);
                    }
                }

                // Вставляем остатки
                if (count % batchSize != 0) {
                    ps.executeBatch();
                }

                // 6. Финализируем таблицу и создаем индексы
                currentStatus.status = "Создание индексов...";
                currentStatus.progress = 95;
                System.out.println("Финализация таблицы...");
                finalizeTable(conn);

                currentStatus.status = "Создание индексов...";
                currentStatus.progress = 97;
                System.out.println("Создание индексов...");
                createIndexes(conn);

                // 7. Обновляем статистику
                currentStatus.status = "Обновление статистики...";
                currentStatus.progress = 99;
                System.out.println("Обновление статистики...");
                updateStatistics(conn);

                // Завершение
                long endTime = System.currentTimeMillis();
                double totalSeconds = (endTime - startTime) / 1000.0;

                currentStatus.isParsing = false;
                currentStatus.status = String.format(
                        "✅ Парсинг завершен за %.1f мин\n" +
                                "Обработано: %,d строк\n" +
                                "Добавлено: %,d записей",
                        totalSeconds / 60, processed, count
                );
                currentStatus.progress = 100;
                currentStatus.processed = totalLines;

                System.out.printf("Парсинг завершен за %.1f минут%n", totalSeconds / 60);
                System.out.printf("Добавлено: %,d записей%n", count);
                System.out.printf("Отфильтровано: %,d%n", filteredByUsername);
                System.out.printf("Пропущено: %,d строк%n", skipped);

            } catch (Exception e) {
                throw new RuntimeException("Ошибка чтения файла: " + e.getMessage(), e);
            }

        } catch (Exception e) {
            throw new RuntimeException("Ошибка парсинга: " + e.getMessage(), e);
        }
    }

    // Вспомогательные методы (ваши оригинальные с небольшими изменениями)
    private long countLines(String filePath) throws Exception {
        long lines = 0;
        try (BufferedReader br = new BufferedReader(new java.io.FileReader(filePath))) {
            while (br.readLine() != null) {
                lines++;
                if (lines % 1000000 == 0) {
                    System.out.println("Подсчитано строк: " + lines);
                }
            }
        }
        return lines;
    }

    private boolean shouldParseLogs(Connection conn, String filePath) throws SQLException {
        System.out.println("Быстрая проверка актуальности...");

        // 1. Проверяем есть ли данные в БД
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM logs")) {

            if (rs.next()) {
                long count = rs.getLong("cnt");
                if (count == 0) {
                    System.out.println("БД пустая - нужно парсить");
                    return true;
                }
            }
        }

        // 2. Быстро проверяем первую запись из БД и первую из файла
        try {
            // Получаем дату первой записи из БД
            LocalDateTime dbFirstDate = null;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT MIN(time) as min_time FROM logs")) {
                if (rs.next()) {
                    Timestamp ts = rs.getTimestamp("min_time");
                    if (ts != null) {
                        dbFirstDate = ts.toLocalDateTime();
                        System.out.println("Минимальная дата в БД: " + dbFirstDate);
                    }
                }
            }

            if (dbFirstDate == null) {
                return true;
            }

            // Быстро читаем начало файла
            try (BufferedReader br = new BufferedReader(new java.io.FileReader(filePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    try {
                        java.util.regex.Matcher m = LOG_PATTERN.matcher(line);
                        if (m.find()) {
                            String rawTime = m.group(1);
                            LocalDateTime fileFirstDate = convertTimestamp(rawTime);

                            if (fileFirstDate != null) {
                                System.out.println("Первая дата в файле: " + fileFirstDate);

                                // Сравниваем только год и месяц
                                boolean samePeriod = dbFirstDate.getYear() == fileFirstDate.getYear() &&
                                        dbFirstDate.getMonthValue() == fileFirstDate.getMonthValue();

                                System.out.println("Данные актуальны (год+месяц совпадают)? " + samePeriod);
                                return !samePeriod; // Если совпадают - не парсить
                            }
                        }
                    } catch (Exception e) {
                        continue;
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("Ошибка при проверке актуальности: " + e.getMessage());
        }

        // В случае ошибки - парсим заново
        return true;
    }

    private void clearLogsTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("TRUNCATE TABLE logs");
        }
    }

    private void createUnloggedTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            // Удаляем временную таблицу если есть
            st.execute("DROP TABLE IF EXISTS logs_unlogged");

            // Создаем UNLOGGED таблицу для быстрой вставки
            String createTableSQL = "CREATE UNLOGGED TABLE logs_unlogged (" +
                    "id BIGSERIAL PRIMARY KEY," +
                    "time TIMESTAMP NOT NULL," +
                    "ip TEXT," +
                    "username TEXT," +
                    "url TEXT," +
                    "status_code INT," +
                    "domain TEXT" +
                    ")";
            st.execute(createTableSQL);
        }
    }

    private void finalizeTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            System.out.println("Финальная обработка таблицы...");

            // 1. Проверяем, существует ли уже таблица logs
            boolean logsTableExists = false;
            try (ResultSet rs = st.executeQuery(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'logs')")) {
                if (rs.next()) {
                    logsTableExists = rs.getBoolean(1);
                }
            }

            if (logsTableExists) {
                System.out.println("Таблица logs уже существует, переименовываем в logs_backup");
                // Переименовываем существующую таблицу
                st.execute("DROP TABLE IF EXISTS logs_backup");
                st.execute("ALTER TABLE IF EXISTS logs RENAME TO logs_backup");
            }

            // 2. Делаем временную таблицу постоянной
            System.out.println("Преобразование UNLOGGED → LOGGED таблицы...");
            st.execute("ALTER TABLE logs_unlogged SET LOGGED");
            st.execute("ALTER TABLE logs_unlogged RENAME TO logs");

            System.out.println("Таблица logs создана успешно");

        } catch (SQLException e) {
            System.err.println("Ошибка при финализации таблицы: " + e.getMessage());
            throw e;
        }
    }

    private void createIndexes(Connection conn) throws SQLException {
        System.out.println("Создание индексов для таблицы logs...");

        try (Statement st = conn.createStatement()) {
            // Список индексов с проверкой существования
            String[][] indexes = {
                    {"idx_logs_time", "CREATE INDEX idx_logs_time ON logs(time)"},
                    {"idx_logs_ip", "CREATE INDEX idx_logs_ip ON logs(ip)"},
                    {"idx_logs_username", "CREATE INDEX idx_logs_username ON logs(username)"},
                    {"idx_logs_status", "CREATE INDEX idx_logs_status ON logs(status_code)"},
                    {"idx_logs_domain", "CREATE INDEX idx_logs_domain ON logs(domain)"},
                    {"idx_logs_url_pattern", "CREATE INDEX idx_logs_url_pattern ON logs(url text_pattern_ops)"}
            };

            // Проверяем существование каждого индекса перед созданием
            for (String[] index : indexes) {
                String indexName = index[0];
                String createSql = index[1];

                // Проверяем существует ли индекс
                boolean indexExists = false;
                try (ResultSet rs = st.executeQuery(
                        "SELECT 1 FROM pg_indexes WHERE tablename = 'logs' AND indexname = '" + indexName + "'")) {
                    indexExists = rs.next();
                } catch (SQLException e) {
                    // Если ошибка - продолжаем
                    System.out.println("Ошибка проверки индекса " + indexName + ": " + e.getMessage());
                }

                if (!indexExists) {
                    try {
                        System.out.println("Создание индекса: " + indexName);
                        st.execute(createSql);
                        System.out.println("Индекс " + indexName + " создан успешно");
                    } catch (SQLException e) {
                        System.err.println("Ошибка создания индекса " + indexName + ": " + e.getMessage());
                        // Продолжаем создавать другие индексы
                    }
                } else {
                    System.out.println("Индекс " + indexName + " уже существует, пропускаем");
                }
            }

            // Создаем материализованное представление
            System.out.println("Создание материализованного представления...");
            try {
                st.execute("DROP MATERIALIZED VIEW IF EXISTS logs_daily_stats");
                st.execute("CREATE MATERIALIZED VIEW logs_daily_stats AS " +
                        "SELECT username, status_code, date_trunc('day', time) as day, count(*) as cnt " +
                        "FROM logs GROUP BY username, status_code, day");
                System.out.println("Материализованное представление создано");
            } catch (SQLException e) {
                System.err.println("Ошибка создания материализованного представления: " + e.getMessage());
            }

            System.out.println("Все индексы созданы успешно");

        } catch (SQLException e) {
            System.err.println("Ошибка при создании индексов: " + e.getMessage());
            throw e;
        }
    }
    private void updateStatistics(Connection conn) throws SQLException {
        System.out.println("Обновление статистики...");

        try (Statement st = conn.createStatement()) {
            // Обновляем статистику PostgreSQL
            st.execute("ANALYZE logs");
            System.out.println("Статистика ANALYZE выполнена");

            // Обновляем материализованное представление
            try {
                st.execute("REFRESH MATERIALIZED VIEW logs_daily_stats");
                System.out.println("Материализованное представление обновлено");
            } catch (SQLException e) {
                System.err.println("Ошибка обновления материализованного представления: " + e.getMessage());
            }

            // Считаем итоговую статистику
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) as total FROM logs")) {
                if (rs.next()) {
                    long totalRows = rs.getLong("total");
                    System.out.println("Итоговое количество записей в таблице logs: " + totalRows);
                }
            }

        } catch (SQLException e) {
            System.err.println("Ошибка при обновлении статистики: " + e.getMessage());
            throw e;
        }
    }

    // Ваши оригинальные методы
    private boolean isValidUsername(String username) {
        if (username == null || username.isEmpty()) return false;
        if (username.toLowerCase().contains("user")) return true;

        int underscoreCount = 0;
        for (char c : username.toCharArray()) {
            if (c == '_') underscoreCount++;
        }
        return underscoreCount >= 2;
    }

    private String extractDomain(String url) {
        if (url == null || url.isEmpty() || url.equals("-")) return null;

        try {
            java.util.regex.Matcher matcher = DOMAIN_PATTERN.matcher(url);
            if (matcher.find()) return matcher.group(1);
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    private LocalDateTime convertTimestamp(String rawTimestamp) {
        try {
            if (rawTimestamp.matches("\\d+(\\.\\d+)?")) {
                double epochSeconds = Double.parseDouble(rawTimestamp);
                long seconds = (long) epochSeconds;
                long nanos = (long) ((epochSeconds - seconds) * 1_000_000_000);
                return LocalDateTime.ofInstant(
                        java.time.Instant.ofEpochSecond(seconds, nanos),
                        java.time.ZoneId.systemDefault()
                );
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    public Map<String, Object> getParsingStatus() {
        System.out.println("Сервис: запрос статуса парсинга");
        System.out.println("Текущий статус: isParsing=" + currentStatus.isParsing +
                ", progress=" + currentStatus.progress +
                ", status=" + currentStatus.status);

        Map<String, Object> status = new HashMap<>();
        status.put("success", true);
        status.put("isParsing", currentStatus.isParsing);
        status.put("status", currentStatus.status);
        status.put("progress", currentStatus.progress);
        status.put("processed", currentStatus.processed);
        status.put("total", currentStatus.total);

        // Рассчитываем оставшееся время если идет парсинг
        if (currentStatus.isParsing && currentStatus.progress > 0 && currentStatus.startTime > 0) {
            long elapsed = System.currentTimeMillis() - currentStatus.startTime;
            double estimatedTotal = elapsed / (currentStatus.progress / 100.0);
            long remaining = (long) ((estimatedTotal - elapsed) / 1000);

            if (remaining < 60) {
                status.put("remaining", "~" + remaining + " сек");
            } else {
                status.put("remaining", "~" + (remaining / 60) + " мин");
            }

            status.put("elapsed", elapsed / 1000); // в секундах
        }

        status.put("filePath", currentStatus.filePath);
        return status;
    }

    // Методы для работы с данными
    public Map<String, Object> getLogsWithStats(int page, int size,
                                                String dateFrom, String dateTo,
                                                String clientIp, String username,
                                                String status, String search) {

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

            // Получаем данные
            String sql = "SELECT id, time, ip, username, url, status_code as statusCode, domain " +
                    "FROM logs " + where + " ORDER BY time DESC LIMIT " + size + " OFFSET " + offset;

            List<Map<String, Object>> logs = jdbcTemplate.queryForList(sql);

            // Общее количество
            String countSql = "SELECT COUNT(*) FROM logs " + where;
            Long totalCount = jdbcTemplate.queryForObject(countSql, Long.class);
            int totalPages = (int) Math.ceil((double) (totalCount != null ? totalCount : 0) / size);

            // Базовая статистика
            Map<String, Object> stats = getBasicStats(where.toString());

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
            result.put("stats", getDefaultStats());
            result.put("totalPages", 1);
        }


        return result;
    }

    private Map<String, Object> getBasicStats(String whereClause) {
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
            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
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
            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
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

        } catch (Exception e) {
            System.err.println("Ошибка получения статистики: " + e.getMessage());
            stats = getDefaultStats();
        }

        return stats;
    }
    // Вспомогательный метод для выполнения COUNT запросов
    private Long executeCountQuery(String sql) {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
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

    private Map<String, Object> getDefaultStats() {
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

    public boolean hasDataInDatabase() {
        try {
            Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM logs LIMIT 1", Long.class);
            return count != null && count > 0;
        } catch (Exception e) {
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

    // Методы для топов
    public List<Map<String, Object>> getTopUrls(int limit) {
        List<Map<String, Object>> result = new ArrayList<>();
        String sql = "SELECT url, domain, COUNT(*) as count " +
                "FROM logs " +
                "WHERE url IS NOT NULL AND url != '' " +
                "GROUP BY url, domain " +
                "ORDER BY count DESC " +
                "LIMIT ?";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
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
                "COUNT(*) as count, " +
                "COUNT(DISTINCT ip) as unique_ips, " +
                "MIN(time) as first_seen, " +
                "MAX(time) as last_seen " +
                "FROM logs " +
                "WHERE username IS NOT NULL AND username != '' AND username != '-' " +
                "GROUP BY username " +
                "ORDER BY count DESC " +
                "LIMIT ?";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setInt(1, limit);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("username", rs.getString("username"));
                    item.put("count", rs.getInt("count"));
                    item.put("unique_ips", rs.getInt("unique_ips"));
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
}