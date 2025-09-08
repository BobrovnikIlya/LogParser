package com.work.LogParser;

import java.io.*;
import java.nio.charset.*;
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
            "^(\\d+\\.\\d+)\\s+" +         // timestamp
                    "\\d+\\s+" +                   // неизвестное число
                    "(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+" +  // IP клиента
                    "(\\S+)/(\\d+)\\s+" +          // тип запроса / статус-код
                    "\\d+\\s+" +                    // размер пакета или число
                    "\\S+\\s+" +                    // CONNECT / GET / POST
                    "(\\S+)\\s+" +                  // URL / host:port
                    "(\\S+|-)"                      // username или '-'
    );


    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final LogRepository logRepository;

    public DataLoader(LogRepository logRepository) {
        this.logRepository = logRepository;
    }

    @Override
    public void run(String... args) throws Exception {


        //if (shouldParseLogs()) {
            System.out.println("Необходимо парсить логи и очистить таблицу.");
            clearLogsTable(); // очистка таблицы
            parseToPostgres(); // парсинг по новой
//        } else {
//            System.out.println("Логи актуальны, парсить не нужно.");
//        }

    }

    private static void parseToPostgres() {
        int count = 0;
        int batchSize = 10000;
        int skipped = 0;

        float startTime = System.currentTimeMillis();
        Charset[] charsetsToTry = new Charset[]{
                StandardCharsets.UTF_8,
                Charset.forName("Windows-1251"),
                StandardCharsets.ISO_8859_1
        };

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {
            // Создаем таблицу, если не существует
            try (Statement st = conn.createStatement()) {
                String createTableSQL = "CREATE TABLE IF NOT EXISTS logs (" +
                        "id BIGSERIAL PRIMARY KEY," +
                        "time TIMESTAMP," +
                        "ip TEXT," +
                        "username TEXT," +
                        "url TEXT," +
                        "status_code INT" +
                        ")";
                st.execute(createTableSQL);
            }

            String insertSQL = "INSERT INTO logs (time, ip, username, url, status_code) VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(insertSQL)) {

                // Открываем файл, пробуем разные кодировки
                BufferedReader br = null;
                for (Charset cs : charsetsToTry) {
                    try {
                        CharsetDecoder decoder = cs.newDecoder()
                                .onMalformedInput(CodingErrorAction.REPORT)
                                .onUnmappableCharacter(CodingErrorAction.REPORT);
                        br = new BufferedReader(new InputStreamReader(new FileInputStream(LOG_FILE.toFile()), decoder));
                        br.readLine(); // тест первой строки
                        br.close();
                        // если дошли сюда без исключения — кодировка подходит
                        decoder = cs.newDecoder()
                                .onMalformedInput(CodingErrorAction.IGNORE)
                                .onUnmappableCharacter(CodingErrorAction.IGNORE);
                        br = new BufferedReader(new InputStreamReader(new FileInputStream(LOG_FILE.toFile()), decoder));
                        System.out.println("Используется кодировка: " + cs.displayName());
                        break;
                    } catch (MalformedInputException e) {
                        // не подходит, пробуем следующую
                        System.out.println("Кодировка " + cs.displayName() + " не подошла.");
                    }
                }

                if (br == null) {
                    System.err.println("Не удалось определить кодировку файла.");
                    return;
                }

                String line;
                while ((line = br.readLine()) != null) {
                    try {
                        Matcher m = LOG_PATTERN.matcher(line);
                        if (m.find()) {
                            // логика разбора полей
                            String rawTime = m.group(1);
                            String ip = m.group(2);
                            int statusCode = Integer.parseInt(m.group(4)); // TCP_TUNNEL/200 -> 200
                            String url = m.group(5);
                            String username = m.group(6);
                            if (username != null) {
                                username = username.trim();
                                if (!username.isEmpty() && !username.equals("-")) {
                                    LocalDateTime dateTime = convertTimestamp(rawTime);
                                    if (dateTime != null) {
                                        ps.setObject(1, dateTime);
                                        ps.setString(2, ip);
                                        ps.setString(3, username);
                                        ps.setString(4, url);
                                        ps.setInt(5, statusCode);
                                        ps.addBatch();
                                        count++;

                                        if (count % batchSize == 0) {
                                            ps.executeBatch();
                                            System.out.println("Записано " + count + " записей.");
                                        }
                                    }
                                }
                            }
                        } else {
                            skipped++;
                            System.out.println("Пропущена строка (не подошла под regex): " + line);
                        }
                    } catch (Exception e) {
                        skipped++;
                        System.out.println("Пропущена строка (ошибка парсинга): " + line);
                    }
                }

                if (count % batchSize != 0) ps.executeBatch();
                System.out.println("Данные успешно записаны в PostgreSQL.");
                System.out.println("Пропущено битых/некорректных строк: " + skipped);
                System.out.println("Время выполнения: " + (System.currentTimeMillis() - startTime)/1000 + " с");

            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
//
//    private static void parseToPostgres() throws IOException {
//        int count = 0;
//        int batchSize = 10000;
//        int skipped = 0; // счётчик битых строк
//        String rawTime = "";
//        String ip = "";
//        float startTime = System.currentTimeMillis();
//        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
//                .onMalformedInput(CodingErrorAction.IGNORE)
//                .onUnmappableCharacter(CodingErrorAction.IGNORE);
//
//        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(LOG_FILE.toFile()), decoder));
//             Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {
//
//            // Создание таблицы с новым полем status_code
//            try (Statement st = conn.createStatement()) {
//                String createTableSQL = "CREATE TABLE IF NOT EXISTS logs (" +
//                        "id BIGSERIAL PRIMARY KEY," +
//                        "time TIMESTAMP," +
//                        "ip TEXT," +
//                        "username TEXT," +
//                        "url TEXT," +
//                        "status_code INT" +
//                        ")";
//                st.execute(createTableSQL);
//            }
//
//            String insertSQL = "INSERT INTO logs (time, ip, username, url, status_code) VALUES (?, ?, ?, ?, ?)";
//            try (PreparedStatement ps = conn.prepareStatement(insertSQL)) {
//                String line;
//                while ((line = br.readLine()) != null) {
//                    try {
//                        Matcher m = LOG_PATTERN.matcher(line);
//                        if (m.find()) {
//                            rawTime = m.group(1);
//                            ip = m.group(2);
//                            String requestType = m.group(3); // TCP_TUNNEL / TCP_DENIED
//                            int statusCode = Integer.parseInt(m.group(4)); // 200, 403, 407...
//                            String url = m.group(5);
//                            String username = m.group(6);
//
//                            LocalDateTime dateTime = convertTimestamp(rawTime);
//                            if (dateTime != null) {
//                                ps.setObject(1, dateTime);
//                                ps.setString(2, ip);
//                                ps.setString(3, username.equals("-") ? null : username);
//                                ps.setString(4, url);
//                                ps.setInt(5, statusCode);
//                                ps.addBatch();
//                                count++;
//
//                                if (count % batchSize == 0) {
//                                    ps.executeBatch();
//                                    System.out.println("Записано " + count + " записей.");
//                                    //if(count > 5000000) break; // ограничитель
//                                }
//                            }
//
//                        } else {
//                            skipped++;
//                            System.out.println("Номер записи " + count + " не подошла под regex: " + rawTime + " user " + ip);
//                        }
//                    } catch (Exception e) {
//                        skipped++;
//                        System.out.println("Номер записи " + count + " ошибка парсинга: " + rawTime + " user " + ip);
//                    }
//                }
//
//                if (count % batchSize != 0) {
//                    ps.executeBatch();
//                    System.out.println("Записано " + count + " записей.");
//                }
//
//                System.out.println("Данные успешно записаны в PostgreSQL.");
//                System.out.println("Пропущено битых/некорректных строк: " + skipped);
//                float endTime = System.currentTimeMillis();
//                System.out.println("Время выполнения: " + (endTime - startTime)/1000 + " с");
//            }
//
//        } catch (IOException | SQLException e) {
//            e.printStackTrace();
//        }
//    }

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
            stmt.executeUpdate("DROP TABLE IF EXISTS logs");
            System.out.println("Таблица logs успешно удалена.");
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