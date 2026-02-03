package com.work.LogParser.service;

import org.springframework.stereotype.Service;

import java.sql.*;

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

    public void finalizeTable(Connection conn) throws SQLException {
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

            try (ResultSet rs = st.executeQuery("SELECT DISTINCT status_code FROM logs WHERE status_code IS NOT NULL")) {
                while (rs.next()) {
                    int statusCode = rs.getInt("status_code");
                    if (statusCode > 0) {
                        saveStatusIfNotExists(conn, statusCode);
                    }
                }
            }

            try (ResultSet rs = st.executeQuery("SELECT DISTINCT action FROM logs WHERE action IS NOT NULL AND action != '-'")) {
                while (rs.next()) {
                    String action = rs.getString("action");
                    if (action != null && !action.trim().isEmpty()) {
                        saveActionIfNotExists(conn, action.trim());
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Ошибка при финализации таблицы: " + e.getMessage());
            throw e;
        }
    }

    public void createIndexes(Connection conn) throws SQLException {
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
                st.execute("""
                        CREATE MATERIALIZED VIEW logs_daily_stats AS\s
                        SELECT username, status_code,\s
                               date_trunc('day', time) as day,\s
                               count(*) as cnt,
                               AVG(response_time_ms) as avg_response_time,
                               SUM(response_size_bytes) as total_traffic_bytes
                        FROM logs\s
                        GROUP BY username, status_code, day""");
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

    public void updateStatistics(Connection conn) throws SQLException {
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