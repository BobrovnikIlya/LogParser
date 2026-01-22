## LogParser (Spring Boot + PostgreSQL)

Анализатор логов proxy-сервера. Бэкенд написан на Spring Boot 3.5.5 (Java 17), данные сохраняются в PostgreSQL. В проекте есть HTML/JS интерфейс (Thymeleaf + статика), однако REST-контроллеры для фронтенда пока не реализованы.

### Что уже реализовано
- Компонент `DataLoader` запускается при старте приложения и парсит локальный файл логов в таблицу PostgreSQL `logs`.
- Разбор строк по регулярному выражению, фильтрация по имени пользователя (минимум два `_` или содержит "user").
- Запись в БД с батч-вставками, индексы на ключевых колонках.
- Материализованное представление `logs_daily_stats` и `ANALYZE` после загрузки.
- Шаблон `templates/index.html` и `static/js/app.js` (UI с фильтрами/графиками) — подготовлены, но ожидают REST API.

### Технологии
- Java 17, Spring Boot 3.5.5: Web, Data JPA, Validation, Thymeleaf
- PostgreSQL (JDBC драйвер)
- Frontend: Vanilla JS + Chart.js (статические файлы из `classpath:/static/`)

### Структура проекта (важное)
- `src/main/java/com/work/LogParser/LogParserApplication.java` — точка входа Spring Boot
- `src/main/java/com/work/LogParser/Components/DataLoader.java` — парсинг лог-файла в PostgreSQL при старте
- `src/main/java/com/work/LogParser/Entities/LogEntry.java` — JPA-сущность `logs`
- `src/main/resources/application.properties` — настройки приложения/БД
- `src/main/resources/templates/index.html` — страница UI
- `src/main/resources/static/js/app.js` — логика UI (ожидает REST API `/api/*`)
- `uploads/` — примеры загруженных логов (локальная папка)

Папка `Controllers/` присутствует, но контроллеров нет. Эндпоинты, к которым обращается фронтенд (`/api/logs`, `/api/upload-log-file`, ...), ещё не добавлены.

### Требования
- JDK 17+
- Maven 3.9+
- PostgreSQL 14+ (локально либо в контейнере)

### Настройка базы данных
Параметры по умолчанию заданы в `src/main/resources/application.properties`:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/ParserLog
spring.datasource.username=postgres
spring.datasource.password=uthgb123
spring.jpa.hibernate.ddl-auto=update
```

Важно: замените пароль и параметры подключения под свою среду. Не храните реальные секреты в репозитории — вынесите в переменные окружения или используйте профиль `application-local.properties`.

### Входной лог-файл
`DataLoader` читает файл по пути, захардкоженному в коде:

```text
D:/logs/notes.txt
```

Изменить путь можно в `Components/DataLoader.java` (константа `LOG_FILE`). Формат строки парсится по регулярному выражению `LOG_PATTERN`, выделяются: время (unix с долями или `yyyy-MM-dd HH:mm:ss.SSS`), IP, статус-код, URL, имя пользователя. Из URL извлекается домен.

### Схема БД и оптимизации
При первом запуске (и в процессе загрузки) создаётся таблица `logs` и индексы, а также материализованное представление `logs_daily_stats`. Hibernate также поддерживает схему по `LogEntry` (`ddl-auto=update`). Колонки: `time`, `ip`, `username`, `url`, `status_code`, `domain` (+ опциональные `response_time`, `response_size`, `action` в сущности).

### Сборка и запуск (Windows PowerShell)
1) Установите PostgreSQL и создайте БД `ParserLog` (или поменяйте URL в настройках)

```powershell
mvn -v
java -version
```

2) Сборка и запуск в dev-режиме:

```powershell
mvn spring-boot:run
```

После старта `DataLoader` проверит свежесть данных и при необходимости:
- очистит таблицу `logs`
- перепарсит файл `D:/logs/notes.txt`

Сборка jar и запуск:

```powershell
mvn clean package
java -jar target/LogParser-0.0.1-SNAPSHOT.jar
```

### Веб-интерфейс
`templates/index.html` и `static/js/app.js` предоставляют UI (фильтры, таблица, графики, экспорт CSV/JSON). Сейчас требуются REST-контроллеры на стороне Spring, к которым обращается фронтенд:
- `GET /api/logs`
- `GET /api/top-urls`
- `GET /api/top-users`
- `POST /api/upload-log-file`
- `POST /api/start-file-parsing`
- `GET /api/parsing-status`

До реализации контроллеров UI будет загружаться, но сетевые запросы вернут 404.

### Известные ограничения
- В `application.properties` и `DataLoader` захардкожены параметры БД/пути — замените на свои
- Формат лога зашит в `LOG_PATTERN`; при отличиях формат нужно адаптировать
- Отсутствуют REST-контроллеры для работы UI

### Планы/рекомендации
- Добавить контроллер(ы) и сервисы под эндпоинты `/api/*`
- Вынести путь входного файла и креды БД в конфигурацию/переменные окружения
- Добавить миграции (Flyway/Liquibase)
- Дополнить тесты (есть заготовки с Testcontainers)

### Лицензия
Укажите лицензию при необходимости.

