-- Создание таблицы logs если не используется Hibernate auto-ddl
CREATE TABLE IF NOT EXISTS logs (
    id BIGSERIAL PRIMARY KEY,
    time TIMESTAMP NOT NULL,
    ip VARCHAR(45),
    username VARCHAR(255),
    url TEXT,
    status_code INTEGER,
    domain VARCHAR(500),
    response_time BIGINT,
    response_size BIGINT,
    action VARCHAR(50)
);

-- Индексы для производительности
CREATE INDEX IF NOT EXISTS idx_logs_time ON logs(time);
CREATE INDEX IF NOT EXISTS idx_logs_username ON logs(username);
CREATE INDEX IF NOT EXISTS idx_logs_status_code ON logs(status_code);
CREATE INDEX IF NOT EXISTS idx_logs_url ON logs(url);
CREATE INDEX IF NOT EXISTS idx_logs_domain ON logs(domain);
CREATE INDEX IF NOT EXISTS idx_logs_ip ON logs(ip);
CREATE INDEX IF NOT EXISTS idx_logs_action ON logs(action);
CREATE INDEX IF NOT EXISTS idx_logs_user_status_time_url ON logs(username, status_code, time, url);

-- Таблица пользователей для автодополнения
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    request_count BIGINT DEFAULT 0,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    last_ip VARCHAR(45)
);

-- Индексы для таблицы users
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_last_seen ON users(last_seen);

-- Материализованное представление для ежедневной статистики (опционально)
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_daily_stats AS
SELECT
    username,
    status_code,
    date_trunc('day', time) as day,
    count(*) as cnt
FROM logs
GROUP BY username, status_code, day;

-- Индекс для материализованного представления
CREATE UNIQUE INDEX IF NOT EXISTS idx_logs_daily_stats_unique
ON logs_daily_stats (username, status_code, day);