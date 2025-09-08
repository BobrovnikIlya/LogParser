package com.work.LogParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.regex.*;
import com.work.LogParser.Repositories.LogRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;



@Component
public class DataLoader implements CommandLineRunner {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/ParserLog";
    private static final String DB_USERNAME = "postgres";
    private static final String DB_PASSWORD = "uthgb123";

    private static final Path LOG_FILE = Paths.get("D:/logs/access.log");   // Входной лог

    private static final Pattern LOG_PATTERN = Pattern.compile(
            "^(\\d+\\.\\d+)\\s+\\d+\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+\\S+\\s+\\d+\\s+\\S+\\s+(\\S+)\\s+(\\S+)"
    );

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final LogRepository logRepository;

    public DataLoader(LogRepository logRepository) {
        this.logRepository = logRepository;
    }

    @Override
    public void run(String... args) throws Exception {
        long startTime = System.currentTimeMillis();

        if (shouldParseLogs()) {
            System.out.println("Необходимо парсить логи и очистить таблицу.");
            //clearLogsTable(); // очистка таблицы
            parseToPostgres(); // парсинг по новой
        } else {
            System.out.println("Логи актуальны, парсить не нужно.");
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Время выполнения: " + (endTime - startTime)/1000 + " с");
    }


    private static void parseToPostgres() {
        int count = 0;
        int batchSize = 10000;

        try (BufferedReader br = Files.newBufferedReader(LOG_FILE, StandardCharsets.UTF_8);
             Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {

            // Создаем таблицу, если не существует
            try (Statement st = conn.createStatement()) {
                String createTableSQL = "CREATE TABLE IF NOT EXISTS logs (" +
                        "id SERIAL PRIMARY KEY," +
                        "time TIMESTAMP," +
                        "ip TEXT," +
                        "url TEXT," +
                        "username TEXT" +
                        ")";
                st.execute(createTableSQL);
            }

            String insertSQL = "INSERT INTO logs (time, ip, url, person) VALUES (?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(insertSQL)) {

                String line;
                while ((line = br.readLine()) != null) {
                    Matcher m = LOG_PATTERN.matcher(line);
                    if (m.find()) {
                        String rawTime = m.group(1);
                        String ip = m.group(2);
                        String url = m.group(3);
                        String username = m.group(4);

                        if (!"-".equals(username)) {
                            LocalDateTime dateTime = convertTimestamp(rawTime);
                            if (dateTime != null) {
                                ps.setObject(1, dateTime); // PostgreSQL корректно принимает LocalDateTime
                                ps.setString(2, ip);
                                ps.setString(3, url);
                                ps.setString(4, username);

                                ps.addBatch();
                                count++;

                                if (count % batchSize == 0) {
                                    ps.executeBatch();
                                    System.out.println("Записано " + count + " записей.");
                                }
                            }
                        }
                    }
                }

                if (count % batchSize != 0) {
                    ps.executeBatch();
                    System.out.println("Записано " + count + " записей.");
                }

                System.out.println("Данные успешно записаны в PostgreSQL.");
            }

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static boolean shouldParseLogs() {
        String query = "SELECT time FROM logs ORDER BY time LIMIT 1";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            if (rs.next()) {
                String firstRecordTime = rs.getString("time");
                LocalDateTime firstRecordDate = convertTimestamp(firstRecordTime);

                if (firstRecordDate == null) return true; // на случай ошибки

                LocalDateTime now = LocalDateTime.now();
                return firstRecordDate.getMonthValue() != now.getMonthValue();
            } else {
                return true;
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return true;
        }
    }

    private static void clearLogsTable() {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DELETE FROM logs");
            System.out.println("Таблица успешно очищена.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Конвертация строки из логов в LocalDateTime
    private static LocalDateTime convertTimestamp(String rawTimestamp) {
        try {
            // Если строка содержит только цифры и точку — считаем как Unix
            if (rawTimestamp.matches("\\d+(\\.\\d+)?")) {
                double epochSeconds = Double.parseDouble(rawTimestamp);
                long seconds = (long) epochSeconds;
                long nanos = (long) ((epochSeconds - seconds) * 1_000_000_000);
                return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanos), ZoneId.systemDefault());
            } else {
                // Иначе парсим как "yyyy-MM-dd HH:mm:ss.SSS"
                return LocalDateTime.parse(rawTimestamp, DATE_FORMATTER);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null; // на случай ошибки
        }
    }




}