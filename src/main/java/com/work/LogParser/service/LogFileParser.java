package com.work.LogParser.service;

import com.work.LogParser.config.DatabaseConfig;
import com.work.LogParser.model.ParsingStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.regex.Pattern;

@Service
public class LogFileParser {

    @Autowired
    private DatabaseManager databaseManager;

    @Autowired
    private LogParserUtils logParserUtils;

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

    public void parseWithOriginalCode(String filePath, ParsingStatus currentStatus) {
        int count = 0;
        int batchSize = 5000;
        int skipped = 0;
        int filteredByUsername = 0;
        long startTime = System.currentTimeMillis();

        System.out.println("Начало парсинга файла: " + filePath);

        try (Connection conn = DriverManager.getConnection(DatabaseConfig.DB_URL, DatabaseConfig.DB_USERNAME, DatabaseConfig.DB_PASSWORD)) {
            databaseManager.ensureLogsTableExists(conn);
            databaseManager.createStatusesTable(conn);
            databaseManager.createActionsTable(conn);

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
            databaseManager.clearLogsTable(conn);

            // 4. Создаем временную таблицу
            System.out.println("Создание временной таблицы...");
            currentStatus.status = "Создание таблиц...";
            databaseManager.createUnloggedTable(conn);

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
                                    // Если статус не указан, пытаемся определить по действию
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
                                    if (logParserUtils.isValidUsername(username)) {
                                        LocalDateTime dateTime = logParserUtils.convertTimestamp(rawTime);
                                        if (dateTime != null) {
                                            String domain = logParserUtils.extractDomain(url);

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

                                            if (statusCode > 0) {
                                                databaseManager.saveStatusIfNotExists(conn, statusCode);
                                            }
                                            databaseManager.saveActionIfNotExists(conn, action);

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
                    databaseManager.finalizeTable(conn);

                    // 7. Создаем индексы
                    System.out.println("Создание индексов...");
                    currentStatus.progress = 97;
                    databaseManager.createIndexes(conn);

                    // 8. Обновляем статистику
                    System.out.println("Обновление статистики...");
                    currentStatus.progress = 99;
                    databaseManager.updateStatistics(conn);

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

    private boolean shouldParseLogs(Connection conn, String filePath) {
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
                            LocalDateTime fileFirstDate = logParserUtils.convertTimestamp(rawTime);

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
}