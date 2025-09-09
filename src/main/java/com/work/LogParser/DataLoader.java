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
//

    private static void parseToPostgres() throws IOException {
        int count = 0;
        int batchSize = 5000;
        int skipped = 0;
        double startTime = System.currentTimeMillis();
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.IGNORE)
                .onUnmappableCharacter(CodingErrorAction.IGNORE);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(LOG_FILE.toFile()), decoder));
             Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {

            try (Statement st = conn.createStatement()) {
                // Создаём UNLOGGED таблицу (быстрее вставка)
                String createTableSQL = "CREATE UNLOGGED TABLE IF NOT EXISTS logs (" +
                        "id BIGSERIAL PRIMARY KEY," +
                        "time TIMESTAMP NOT NULL," +
                        "ip TEXT," +
                        "username TEXT," +
                        "url TEXT," +
                        "status_code INT" +
                        ")";
                st.execute(createTableSQL);

                // Индексы
                st.execute("CREATE INDEX IF NOT EXISTS idx_logs_time ON logs(time)");
                st.execute("CREATE INDEX IF NOT EXISTS idx_logs_username ON logs(username)");
                st.execute("CREATE INDEX IF NOT EXISTS idx_logs_status_code ON logs(status_code)");
                st.execute("CREATE INDEX IF NOT EXISTS idx_logs_url ON logs(url)");
                st.execute("CREATE INDEX IF NOT EXISTS idx_logs_user_status_time_url ON logs(username, status_code, time, url)");

                // Материализованное представление (агрегация по дням)
                String createMV = "CREATE MATERIALIZED VIEW IF NOT EXISTS logs_daily_stats AS " +
                        "SELECT username, status_code, date_trunc('day', time) as day, count(*) as cnt " +
                        "FROM logs GROUP BY username, status_code, day";
                st.execute(createMV);
            }

            String insertSQL = "INSERT INTO logs (time, ip, username, url, status_code) VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(insertSQL)) {
                String line;
                while ((line = br.readLine()) != null) {
                    try {
                        Matcher m = LOG_PATTERN.matcher(line);
                        if (m.find()) {
                            String rawTime = m.group(1);
                            String ip = m.group(2);
                            int statusCode = Integer.parseInt(m.group(4));
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
                                            //if(count >=200000) break;
                                        }
                                    }
                                }
                            }
                        } else {
                            skipped++;
                            System.out.println("Пропущена строка (не regex): " + line);
                        }
                    } catch (Exception e) {
                        skipped++;
                        System.out.println("Ошибка парсинга: " + line);
                    }
                }

                if (count % batchSize != 0) {
                    ps.executeBatch();
                    System.out.println("Записано " + count + " записей.");
                }
            }

            // После загрузки делаем ANALYZE (обновляем статистику)
            try (Statement st = conn.createStatement()) {
                st.execute("ANALYZE logs");
                // Освежаем материализованное представление
                st.execute("REFRESH MATERIALIZED VIEW logs_daily_stats");
                // Делаем таблицу снова LOGGED для обычного поведения транзакций
                st.execute("ALTER TABLE logs SET LOGGED");
                System.out.println("Таблица logs снова LOGGED. Все готово.");
            }

            System.out.println("Данные успешно записаны.");
            System.out.println("Пропущено строк: " + skipped);
            double endTime = System.currentTimeMillis();
            System.out.println("Время: " + (endTime - startTime) / 1000 + " с");

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
            stmt.executeUpdate("TRUNCATE TABLE logs");
            System.out.println("Таблица logs успешно очищена.");
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