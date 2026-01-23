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
            "^" +
                    "(\\d+\\.\\d+)\\s+" +              // 1. timestamp
                    "(\\d+)\\s+" +                     // 2. response_time_ms
                    "(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+" + // 3. client_ip
                    "([A-Z_]+)(?:/(\\d{3}))?\\s+" +    // 4. action и 5. status_code (опционально)
                    "(\\d+)\\s+" +                     // 6. response_size_bytes
                    "(\\S+)\\s+" +                     // 7. http_method
                    "(\\S+)\\s+" +                     // 8. url
                    "(\\S+|-)\\s+" +                   // 9. username
                    "(\\S+)\\s+" +                     // 10. hierarchy
                    "(\\S+)"                           // 11. content_type
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
        int count = 0;
        int batchSize = 5000;
        int skipped = 0;
        int filteredByUsername = 0;
        long startTime = System.currentTimeMillis();

        System.out.println("Начало парсинга файла: " + filePath);

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {
            ensureLogsTableExists(conn);
            // 1. Проверяем нужно ли парсить
            if (!shouldParseLogs(conn, filePath)) {
                System.out.println("Парсинг не требуется - данные актуальны");
                currentStatus.status = "Парсинг не требуется - данные актуальны";
                currentStatus.progress = 100;
                currentStatus.isParsing = false;
                return;
            }

            // 2. Считаем строки
            System.out.println("Подсчет строк...");
            currentStatus.status = "Подсчет строк...";
            long totalLines = countLines(filePath);
            currentStatus.total = totalLines;
            System.out.println("Всего строк в файле: " + totalLines);

            // 3. Очищаем таблицу
            System.out.println("Очистка таблицы...");
            currentStatus.status = "Очистка таблицы...";
            clearLogsTable(conn);

            // 4. Создаем временную таблицу
            System.out.println("Создание временной таблицы...");
            currentStatus.status = "Создание таблиц...";
            createUnloggedTable(conn);

            // 5. Парсим файл
            System.out.println("Начало парсинга строк...");
            currentStatus.status = "Парсинг файла...";

            try (BufferedReader br = new BufferedReader(new java.io.FileReader(filePath));
                 PreparedStatement ps = conn.prepareStatement(
                         "INSERT INTO logs_unlogged (time, ip, username, url, status_code, domain, response_time_ms, response_size_bytes, action) " +
                                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {

                String line;
                long processed = 0;
                int lineNumber = 0;

                while ((line = br.readLine()) != null) {
                    processed++;
                    lineNumber++;

                    // Показываем прогресс каждые 10000 строк
                    if (processed % 10000 == 0) {
                        System.out.println("Обработано строк: " + processed +
                                ", добавлено записей: " + count +
                                ", пропущено: " + skipped);
                    }

                    try {
                        java.util.regex.Matcher m = LOG_PATTERN.matcher(line);

                        if (m.find()) {
                            String rawTime = m.group(1);
                            int responseTimeMs = Integer.parseInt(m.group(2));
                            String ip = m.group(3);
                            String action = m.group(4);           // Действие (TCP_TUNNEL, TCP_MISS и т.д.)
                            String statusStr = m.group(5);        // Код статуса (может быть null)

                            int statusCode = 0;
                            try {
                                if (statusStr != null && !statusStr.isEmpty()) {
                                    statusCode = Integer.parseInt(statusStr);
                                } else {
                                    // Если статус не указан, пытаемся определить по дейаствию
                                    if (action.contains("DENIED") || action.contains("DENY")) {
                                        statusCode = 403;
                                    } else if (action.contains("MISS")) {
                                        statusCode = 200; // Предполагаем успех для MISS
                                    } else if (action.contains("HIT")) {
                                        statusCode = 200;
                                    }
                                }
                            } catch (Exception e) {
                                System.err.println("Ошибка парсинга статуса: " + statusStr);
                                statusCode = 0;
                            }

                            long responseSizeBytes = Long.parseLong(m.group(6));
                            String httpMethod = m.group(7);
                            String url = m.group(8);
                            String username = m.group(9);

                            if (username != null && !username.equals("-")) {
                                username = username.trim();
                                if (!username.isEmpty()) {
                                    if (isValidUsername(username)) {
                                        LocalDateTime dateTime = convertTimestamp(rawTime);
                                        if (dateTime != null) {
                                            String domain = extractDomain(url);

                                            // ДЕБАГ: выводим первую найденную строку
                                            if (count == 0) {
                                                System.out.println("ПЕРВАЯ УСПЕШНО РАСПАРСЕННАЯ СТРОКА:");
                                                System.out.println("Строка: " + line);
                                                System.out.println("Группы:");
                                                for (int i = 1; i <= m.groupCount(); i++) {
                                                    System.out.println("  Группа " + i + ": '" + m.group(i) + "'");
                                                }
                                            }

                                            ps.setObject(1, dateTime);
                                            ps.setString(2, ip);
                                            ps.setString(3, username);
                                            ps.setString(4, url);
                                            ps.setInt(5, statusCode);
                                            ps.setString(6, domain);
                                            ps.setInt(7, responseTimeMs);
                                            ps.setLong(8, responseSizeBytes);
                                            ps.setString(9, action);

                                            ps.addBatch();
                                            count++;

                                            if (count % batchSize == 0) {
                                                ps.executeBatch();
                                                System.out.println("Выполнен batch из " + batchSize + " записей");
                                            }
                                        } else {
                                            System.out.println("ОШИБКА: Не удалось преобразовать timestamp: " + rawTime);
                                            skipped++;
                                        }
                                    } else {
                                        filteredByUsername++;
                                    }
                                }
                            }
                        } else {
                            // ДЕБАГ: выводим первую НЕраспарсенную строку
                            if (skipped == 0) {
                                System.out.println("ПЕРВАЯ НЕРАСПАРСЕННАЯ СТРОКА:");
                                System.out.println("Строка: " + line);
                                System.out.println("Длина строки: " + line.length());
                            }
                            skipped++;
                        }
                    } catch (Exception e) {
                        // ДЕБАГ: выводим первую ошибку парсинга
                        if (skipped == 0) {
                            System.out.println("ПЕРВАЯ ОШИБКА ПАРСИНГА в строке " + lineNumber + ":");
                            System.out.println("Строка: " + line);
                            System.out.println("Ошибка: " + e.getMessage());
                            e.printStackTrace();
                        }
                        skipped++;
                    }

                    // Обновляем прогресс
                    currentStatus.processed = processed;
                    currentStatus.progress = (processed * 100.0) / totalLines;
                }

                // Вставляем остатки
                if (count % batchSize != 0) {
                    ps.executeBatch();
                    System.out.println("Выполнен финальный batch из " + (count % batchSize) + " записей");
                }

                System.out.println("Парсинг завершен. Статистика:");
                System.out.println("  Всего строк в файле: " + processed);
                System.out.println("  Успешно распарсено: " + count);
                System.out.println("  Отфильтровано по username: " + filteredByUsername);
                System.out.println("  Пропущено (ошибки): " + skipped);

                if (count == 0) {
                    System.out.println("ВНИМАНИЕ: Не добавлено ни одной записи в БД!");
                    System.out.println("Возможные причины:");
                    System.out.println("1. Неверный формат логов");
                    System.out.println("2. Неправильное регулярное выражение");
                    System.out.println("3. Все имена пользователей отфильтрованы");
                }

                // Продолжаем финализацию только если есть данные
                if (count > 0) {
                    // 6. Финализируем таблицу
                    System.out.println("Финализация таблицы...");
                    currentStatus.progress = 95;
                    finalizeTable(conn);

                    // 7. Создаем индексы
                    System.out.println("Создание индексов...");
                    currentStatus.progress = 97;
                    createIndexes(conn);

                    // 8. Обновляем статистику
                    System.out.println("Обновление статистики...");
                    currentStatus.progress = 99;
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

                    System.out.printf("Парсинг успешно завершен за %.1f минут%n", totalSeconds / 60);

                } else {
                    // Если данных нет - просто завершаем
                    currentStatus.isParsing = false;
                    currentStatus.status = "❌ Не удалось добавить записи в БД. Проверьте формат логов.";
                    currentStatus.progress = 100;
                    System.out.println("Парсинг завершен без добавления записей в БД");
                }

            } catch (Exception e) {
                System.err.println("Ошибка чтения файла: " + e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Ошибка чтения файла: " + e.getMessage(), e);
            }

        } catch (Exception e) {
            System.err.println("Ошибка парсинга: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Ошибка парсинга: " + e.getMessage(), e);
        }
    }
    private void ensureLogsTableExists(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Проверяем существование таблицы logs
            boolean tableExists = false;
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'logs')")) {
                if (rs.next()) {
                    tableExists = rs.getBoolean(1);
                }
            }

            if (!tableExists) {
                System.out.println("Таблица logs не существует, создаем новую...");

                // Создаем таблицу logs с полной структурой
                String createTableSQL = "CREATE TABLE logs (" +
                        "id BIGSERIAL PRIMARY KEY," +
                        "time TIMESTAMP NOT NULL," +
                        "ip TEXT," +
                        "username TEXT," +
                        "url TEXT," +
                        "status_code INT," +
                        "domain TEXT," +
                        "response_time_ms INT," +
                        "response_size_bytes BIGINT," +
                        "action TEXT," +
                        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                        ")";

                stmt.execute(createTableSQL);
                System.out.println("Таблица logs успешно создана");

                // Сразу создаем базовые индексы
                createBasicIndexes(conn);

                // Создаем последовательность для id
                stmt.execute("CREATE SEQUENCE IF NOT EXISTS logs_id_seq START 1");
                System.out.println("Последовательность logs_id_seq создана");

            } else {
                System.out.println("Таблица logs уже существует");
            }
        } catch (SQLException e) {
            System.err.println("Ошибка при создании/проверке таблицы logs: " + e.getMessage());
            throw e;
        }
    }

    private void createBasicIndexes(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            System.out.println("Создание базовых индексов для таблицы logs...");

            // Создаем только самые важные индексы для начала
            String[] indexQueries = {
                    "CREATE INDEX IF NOT EXISTS idx_logs_time ON logs(time)",
                    "CREATE INDEX IF NOT EXISTS idx_logs_ip ON logs(ip)",
                    "CREATE INDEX IF NOT EXISTS idx_logs_username ON logs(username)",
                    "CREATE INDEX IF NOT EXISTS idx_logs_status ON logs(status_code)",
                    "CREATE INDEX IF NOT EXISTS idx_logs_domain ON logs(domain)",
                    "CREATE INDEX IF NOT EXISTS idx_logs_action ON logs(action)"
            };

            for (String query : indexQueries) {
                try {
                    stmt.execute(query);
                    System.out.println("Индекс создан: " + query.split(" ")[3]);
                } catch (SQLException e) {
                    // Если индекс уже существует, пропускаем
                    if (e.getMessage().contains("already exists")) {
                        System.out.println("Индекс уже существует: " + query.split(" ")[3]);
                    } else {
                        System.err.println("Ошибка создания индекса: " + e.getMessage());
                    }
                }
            }
            System.out.println("Базовые индексы созданы");
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
        } catch (SQLException e) {
            // Если произошла ошибка при проверке (например, таблицы нет)
            System.out.println("Ошибка проверки БД, считаем что нужно парсить: " + e.getMessage());
            return true;
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
            // Проверяем существование таблицы перед очисткой
            boolean tableExists = false;
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'logs')")) {
                if (rs.next()) {
                    tableExists = rs.getBoolean(1);
                }
            }

            if (tableExists) {
                stmt.executeUpdate("TRUNCATE TABLE logs");
                System.out.println("Таблица logs очищена");
            } else {
                System.out.println("Таблица logs не существует, очистка не требуется");
            }
        }
    }

    private void createUnloggedTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS logs_unlogged");

            String createTableSQL = "CREATE UNLOGGED TABLE logs_unlogged (" +
                    "id BIGSERIAL PRIMARY KEY," +
                    "time TIMESTAMP NOT NULL," +
                    "ip TEXT," +
                    "username TEXT," +
                    "url TEXT," +
                    "status_code INT," +
                    "domain TEXT," +
                    "response_time_ms INT," +          // Время ответа
                    "response_size_bytes BIGINT," +     // Размер ответа
                    "action TEXT" +                     // Действие proxy
                    ")";
            st.execute(createTableSQL);
            System.out.println("Создана таблица logs_unlogged с колонкой action");
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
                    {"idx_logs_url_pattern", "CREATE INDEX idx_logs_url_pattern ON logs(url text_pattern_ops)"},
                    {"idx_logs_response_time", "CREATE INDEX idx_logs_response_time ON logs(response_time_ms) WHERE response_time_ms > 0"},
                    {"idx_logs_response_size", "CREATE INDEX idx_logs_response_size ON logs(response_size_bytes) WHERE response_size_bytes > 0"},
                    {"idx_logs_action", "CREATE INDEX idx_logs_action ON logs(action)"}
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
                st.execute( "DROP MATERIALIZED VIEW IF EXISTS logs_daily_stats");
                st.execute( "CREATE MATERIALIZED VIEW logs_daily_stats AS \n" +
                                "SELECT username, status_code, \n" +
                                "       date_trunc('day', time) as day, \n" +
                                "       count(*) as cnt,\n" +
                                "       AVG(response_time_ms) as avg_response_time,\n" +
                                "       SUM(response_size_bytes) as total_traffic_bytes\n" +
                                "FROM logs \n" +
                                "GROUP BY username, status_code, day");
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

            if (!logs.isEmpty()) {
                System.out.println("Первая запись лога (поля): " + logs.get(0).keySet());
                System.out.println("Первая запись лога (значения):");
                for (Map.Entry<String, Object> entry : logs.get(0).entrySet()) {
                    System.out.println("  " + entry.getKey() + ": " + entry.getValue() +
                            " (тип: " + (entry.getValue() != null ? entry.getValue().getClass().getName() : "null") + ")");
                }
            }

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

    private Double executeDoubleQuery(String sql) {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
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

    private Long executeLongQuery(String sql) {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
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
                "ip, " +  // <-- ДОБАВЛЕНО: берем IP пользователя
                "COUNT(*) as count, " +
                "MIN(time) as first_seen, " +
                "MAX(time) as last_seen " +
                "FROM logs " +
                "WHERE username IS NOT NULL AND username != '' AND username != '-' " +
                "GROUP BY username, ip " +  // <-- ИЗМЕНЕНО: группируем по username и ip
                "ORDER BY count DESC " +
                "LIMIT ?";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
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
}