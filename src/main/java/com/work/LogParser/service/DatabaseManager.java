package com.work.LogParser.service;

import com.work.LogParser.config.DatabaseConfig;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

@Service
public class DatabaseManager {

    public void ensureLogsTableExists(Connection conn) throws SQLException {
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

    public long createIndexesWithProgressTracking(Connection conn, Consumer<Integer> progressCallback) throws SQLException {
        long startTime = System.currentTimeMillis();
        System.out.println("Создание индексов с отслеживанием прогресса...");

        IndexTask[] indexTasks = {
                new IndexTask("CREATE INDEX IF NOT EXISTS idx_logs_time ON logs(time)", 1, false),
                new IndexTask("CREATE INDEX IF NOT EXISTS idx_logs_username ON logs(username)", 1, false),
                new IndexTask("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_logs_active_users ON logs(username) WHERE username != '-'", 2, true),
                new IndexTask("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_logs_error_status ON logs(status_code, time) WHERE status_code >= 400", 2, true),
                new IndexTask("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_logs_ip_filter ON logs(ip) WHERE ip IS NOT NULL", 2, true),
                new IndexTask("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_logs_large_files ON logs(response_size_bytes, url) WHERE response_size_bytes > 1048576", 3, true)
        };

        int totalWeight = Arrays.stream(indexTasks).mapToInt(task -> task.weight).sum();
        final int[] currentWeight = {0};

        for (IndexTask task : indexTasks) {
            if (!task.concurrent) {
                try (Statement st = conn.createStatement()) {
                    st.execute(task.sql);
                    currentWeight[0] += task.weight;
                    if (progressCallback != null) {
                        progressCallback.accept((currentWeight[0] * 100) / totalWeight);
                    }
                }
            }
        }

        Thread backgroundIndexing = new Thread(() -> {
            try (Connection bgConn = DriverManager.getConnection(
                    DatabaseConfig.DB_URL, DatabaseConfig.DB_USERNAME, DatabaseConfig.DB_PASSWORD)) {
                bgConn.setAutoCommit(true);
                for (IndexTask task : indexTasks) {
                    if (task.concurrent) {
                        try (Statement bgStmt = bgConn.createStatement()) {
                            bgStmt.execute(task.sql);
                            currentWeight[0] += task.weight;
                            if (progressCallback != null) {
                                progressCallback.accept((currentWeight[0] * 100) / totalWeight);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Ошибка в фоновом создании индексов: " + e.getMessage());
            }
        });

        backgroundIndexing.start();
        try {
            backgroundIndexing.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Все индексы созданы за " + duration + " мс");
        return duration;
    }
    // Вспомогательный класс для задания на создание индекса
    private static class IndexTask {
        String sql;
        int weight; // Вес индекса (сложность создания, 1-3)
        boolean concurrent; // Создается ли конкурентно

        IndexTask(String sql, int weight, boolean concurrent) {
            this.sql = sql;
            this.weight = weight;
            this.concurrent = concurrent;
        }
    }

    public void clearLogsTable(Connection conn) throws SQLException {
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

    public void createUnloggedTable(Connection conn) throws SQLException {
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

    public long finalizeTable(Connection conn) throws SQLException {
        long startTime = System.currentTimeMillis();
        System.out.println("Финальная обработка таблицы...");

        boolean originalAutoCommit = conn.getAutoCommit();

        try (Statement st = conn.createStatement()) {
            if (originalAutoCommit) {
                conn.setAutoCommit(false);
            }

            st.execute("ALTER TABLE logs_unlogged SET LOGGED");
            st.execute("DROP TABLE IF EXISTS logs_old");
            st.execute("ALTER TABLE IF EXISTS logs RENAME TO logs_old");
            st.execute("ALTER TABLE logs_unlogged RENAME TO logs");
            populateStatusesAndActions(conn);

            if (originalAutoCommit) {
                conn.commit();
                conn.setAutoCommit(true);
            }

            long duration = System.currentTimeMillis() - startTime;
            System.out.println("Таблица logs заменена атомарно за " + duration + " мс");
            return duration;

        } catch (Exception e) {
            if (!originalAutoCommit) {
                conn.rollback();
            }
            throw e;
        }
    }

    public void updateStatistics(Connection conn) throws SQLException {
        System.out.println("Обновление статистики...");

        try (Statement st = conn.createStatement()) {
            // 1. ANALYZE для основной таблицы
            st.execute("ANALYZE logs");
            System.out.println("Статистика ANALYZE выполнена");

            // 2. Проверяем и обновляем материализованные представления
            String[] views = {"mv_top_urls", "mv_top_users"};

            for (String view : views) {
                try {
                    // Проверяем существует ли
                    boolean exists = false;
                    try (ResultSet rs = st.executeQuery(
                            "SELECT EXISTS (SELECT FROM pg_matviews WHERE matviewname = '" + view + "')"
                    )) {
                        if (rs.next()) exists = rs.getBoolean(1);
                    }

                    if (exists) {
                        st.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY " + view);
                        System.out.println("Представление " + view + " обновлено");
                    }
                } catch (Exception e) {
                    System.err.println("Ошибка обновления " + view + ": " + e.getMessage());
                }
            }

            // 3. Статистика по количеству записей
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

    public void createStatusesTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Проверяем существование таблицы
            boolean tableExists = false;
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'log_statuses')")) {
                if (rs.next()) {
                    tableExists = rs.getBoolean(1);
                }
            }

            if (!tableExists) {
                // Создаем таблицу для хранения уникальных статусов
                String createSQL = "CREATE TABLE log_statuses (" +
                        "status_code INT PRIMARY KEY," +
                        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                        ")";
                stmt.execute(createSQL);
                System.out.println("Таблица log_statuses создана");

                // Создаем индекс для быстрого поиска
                stmt.execute("CREATE INDEX idx_log_statuses_code ON log_statuses(status_code)");
            }
        }
    }

    public void saveStatusIfNotExists(Connection conn, int statusCode) {
        if (statusCode <= 0) return;

        String sql = "INSERT INTO log_statuses (status_code) VALUES (?) " +
                "ON CONFLICT (status_code) DO NOTHING";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, statusCode);
            ps.executeUpdate();
        } catch (SQLException e) {
            // Игнорируем если таблицы нет или другие не критичные ошибки
            System.err.println("Не удалось сохранить статус " + statusCode + ": " + e.getMessage());
        }
    }

    public void createActionsTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Проверяем существование таблицы
            boolean tableExists = false;
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'log_actions')")) {
                if (rs.next()) {
                    tableExists = rs.getBoolean(1);
                }
            }

            if (!tableExists) {
                // Создаем простую таблицу только для хранения уникальных actions
                String createSQL = "CREATE TABLE log_actions (" +
                        "action TEXT PRIMARY KEY," +
                        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                        ")";
                stmt.execute(createSQL);
                System.out.println("Таблица log_actions создана");

                // Создаем индекс для быстрого поиска
                stmt.execute("CREATE INDEX idx_log_actions_action ON log_actions(action)");
            }
        }
    }

    public void saveActionIfNotExists(Connection conn, String action) {
        if (action == null || action.isEmpty() || action.equals("-")) return;

        String sql = "INSERT INTO log_actions (action) VALUES (?) " +
                "ON CONFLICT (action) DO NOTHING";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, action.trim());
            ps.executeUpdate();
        } catch (SQLException e) {
            // Игнорируем если таблицы нет или другие не критичные ошибки
            System.err.println("Не удалось сохранить action '" + action + "': " + e.getMessage());
        }
    }

    private void populateStatusesAndActions(Connection conn) throws SQLException {
        System.out.println("Заполнение таблиц статусов и действий...");

        // 1. Заполняем статусы
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT DISTINCT status_code FROM logs WHERE status_code IS NOT NULL AND status_code > 0"
             )) {
            int count = 0;
            while (rs.next()) {
                int statusCode = rs.getInt("status_code");
                saveStatusIfNotExists(conn, statusCode);
                count++;
            }
            System.out.println("Добавлено статусов: " + count);
        }

        // 2. Заполняем действия
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT DISTINCT action FROM logs WHERE action IS NOT NULL AND action != '-'"
             )) {
            int count = 0;
            while (rs.next()) {
                String action = rs.getString("action");
                if (action != null && !action.trim().isEmpty()) {
                    saveActionIfNotExists(conn, action.trim());
                    count++;
                }
            }
            System.out.println("Добавлено действий: " + count);
        }
    }

    public void prepareConnectionForCopy(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            System.out.println("Оптимизация соединения для быстрого COPY...");

            // Убираем настройки, которые нельзя изменить во время транзакции
            // stmt.execute("SET full_page_writes = off"); // УДАЛЯЕМ ЭТУ СТРОКУ

            // Оставляем только безопасные настройки:

            // 1. Отключаем синхронный коммит (ускоряет запись)
            stmt.execute("SET synchronous_commit = off");

            // 2. Увеличиваем рабочую память
            stmt.execute("SET maintenance_work_mem = '1GB'");
            stmt.execute("SET work_mem = '128MB'");

            // 3. Убираем ограничения по времени
            stmt.execute("SET statement_timeout = 0");
            stmt.execute("SET lock_timeout = 0");

            // 4. Включаем параллельные запросы если есть много ядер
            stmt.execute("SET max_parallel_workers_per_gather = 4");

            // 5. Дополнительные безопасные настройки
            stmt.execute("SET commit_delay = 100000");  // Задержка коммита для группировки
            stmt.execute("SET commit_siblings = 5");    // Минимальное количество параллельных транзакций для задержки

            System.out.println("Соединение оптимизировано для COPY");
        } catch (SQLException e) {
            System.err.println("Ошибка при оптимизации соединения: " + e.getMessage());
            // Продолжаем работу даже если не удалось изменить настройки
        }
    }

    /**
     * Восстановление нормальных настроек после COPY (исправленная версия)
     */
    public void restoreConnectionSettings(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            System.out.println("Восстановление настроек соединения...");

            stmt.execute("SET synchronous_commit = on");
            // stmt.execute("SET full_page_writes = on"); // УДАЛЯЕМ ЭТУ СТРОКУ
            stmt.execute("RESET commit_delay");
            stmt.execute("RESET commit_siblings");
            stmt.execute("RESET maintenance_work_mem");
            stmt.execute("RESET work_mem");

            System.out.println("Настройки соединения восстановлены");
        } catch (SQLException e) {
            System.err.println("Ошибка при восстановлении настроек: " + e.getMessage());
            // Игнорируем ошибку, так как это не критично
        }
    }
}