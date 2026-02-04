package com.work.LogParser.controller;

import com.work.LogParser.service.LogParsingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class ParsingController {

    @Autowired
    private LogParsingService logParsingService;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @PostMapping("/start-file-parsing")
    public ResponseEntity<?> startFileParsing(@RequestBody Map<String, String> request) {
        System.out.println("=== ЗАПРОС НА ПАРСИНГ ===");
        System.out.println("Получен запрос: " + request);

        try {
            String filePath = request.get("filePath");
            System.out.println("Путь к файлу: " + filePath);

            if (filePath == null || filePath.isEmpty()) {
                System.out.println("Ошибка: путь к файлу не указан");
                return ResponseEntity.badRequest().body(
                        Map.of("success", false, "error", "Путь к файлу не указан")
                );
            }

            // Проверяем существование файла
            java.nio.file.Path path = java.nio.file.Paths.get(filePath);
            if (!java.nio.file.Files.exists(path)) {
                System.out.println("Ошибка: файл не найден - " + filePath);
                return ResponseEntity.badRequest().body(
                        Map.of("success", false, "error", "Файл не найден: " + filePath)
                );
            }

            System.out.println("Файл найден, размер: " + java.nio.file.Files.size(path) + " байт");

            Map<String, Object> response = new HashMap<>();

            if (logParsingService.startParsing(filePath)) {
                response.put("success", true);
                response.put("message", "Парсинг запущен");
                response.put("filePath", filePath);
                System.out.println("Парсинг успешно запущен");
            } else {
                response.put("success", false);
                response.put("error", "Парсинг уже выполняется");
                System.out.println("Ошибка: парсинг уже выполняется");
            }

            System.out.println("Отправляем ответ: " + response);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            System.err.println("Исключение при запуске парсинга: " + e.getMessage());
            e.printStackTrace();
            return ResponseEntity.status(500).body(Map.of(
                    "success", false,
                    "error", "Ошибка запуска: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/parsing-status")
    public ResponseEntity<?> getParsingStatus() {
        System.out.println("=== ЗАПРОС СТАТУСА ПАРСИНГА ===");

        try {
            Map<String, Object> status = logParsingService.getParsingStatus();
            System.out.println("Текущий статус: " + status);

            return ResponseEntity.ok(status);

        } catch (Exception e) {
            System.err.println("Ошибка получения статуса: " + e.getMessage());
            return ResponseEntity.ok(Map.of(
                    "success", false,
                    "error", "Ошибка получения статуса: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/check-data")
    public ResponseEntity<?> checkData() {
        boolean hasData = logParsingService.hasDataInDatabase();
        long count = logParsingService.getLogCount();

        return ResponseEntity.ok(Map.of(
                "success", true,
                "hasData", hasData,
                "count", count
        ));
    }

    @GetMapping("/logs")
    public ResponseEntity<?> getLogs(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "50") int size,
            @RequestParam(required = false) String dateFrom,
            @RequestParam(required = false) String dateTo,
            @RequestParam(required = false) String clientIp,
            @RequestParam(required = false) String username,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String search,
            @RequestParam(required = false) String action) {

        try {
            Map<String, Object> result = logParsingService.getLogsWithStats(
                    page, size, dateFrom, dateTo, clientIp, username, status, search, action
            );

            Map<String, Object> response = new HashMap<>(result);
            response.put("success", true);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of(
                    "success", false,
                    "error", "Ошибка загрузки: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/api/quick-logs")
    public Map<String, Object> getQuickLogs(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "50") int size) {

        // Ограничиваем размер для безопасности
        size = Math.min(size, 100);

        Map<String, Object> result = new HashMap<>();

        try {
            int offset = (page - 1) * size;

            // ТОЛЬКО данные, без статистики (для скорости)
            String sql = "SELECT id, time, ip, username, url, status_code as statusCode, domain " +
                    "FROM logs ORDER BY time DESC LIMIT " + size + " OFFSET " + offset;

            List<Map<String, Object>> logs = jdbcTemplate.queryForList(sql);

            // Быстрый подсчет общего количества
            Long totalCount = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM logs", Long.class
            );

            int totalPages = (int) Math.ceil((double) (totalCount != null ? totalCount : 0) / size);

            result.put("success", true);
            result.put("logs", logs);
            result.put("totalPages", totalPages);
            result.put("currentPage", page);
            result.put("totalRecords", totalCount != null ? totalCount : 0);

        } catch (Exception e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }

        return result;
    }

    @GetMapping("/top-urls")
    public ResponseEntity<?> getTopUrls(
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(required = false) String dateFrom,
            @RequestParam(required = false) String dateTo,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) String username,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String action) {

        try {
            // Используем метод с фильтрами
            var topUrls = logParsingService.getTopUrlsWithFilters(limit, dateFrom, dateTo,
                    ip, username, status, action);

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "data", topUrls,
                    "count", topUrls.size(),
                    "filters", Map.of(
                            "dateFrom", dateFrom != null ? dateFrom : "all",
                            "dateTo", dateTo != null ? dateTo : "all",
                            "ip", ip != null ? ip : "all",
                            "username", username != null ? username : "all",
                            "status", status != null ? status : "all",
                            "action", action != null ? action : "all"
                    )
            ));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of(
                    "success", false,
                    "error", "Ошибка: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/top-users")
    public ResponseEntity<?> getTopUsers(
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(required = false) String dateFrom,
            @RequestParam(required = false) String dateTo,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) String username,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String action) {

        try {
            // Используем метод с фильтрами
            var topUsers = logParsingService.getTopUsersWithFilters(limit, dateFrom, dateTo,
                    ip, username, status, action);

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "data", topUsers,
                    "count", topUsers.size(),
                    "filters", Map.of(
                            "dateFrom", dateFrom != null ? dateFrom : "all",
                            "dateTo", dateTo != null ? dateTo : "all",
                            "ip", ip != null ? ip : "all",
                            "username", username != null ? username : "all",
                            "status", status != null ? status : "all",
                            "action", action != null ? action : "all"
                    )
            ));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of(
                    "success", false,
                    "error", "Ошибка: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/combined-tops")
    public ResponseEntity<?> getCombinedTops(
            @RequestParam(defaultValue = "100") int urlLimit,
            @RequestParam(defaultValue = "10") int userLimit,
            @RequestParam(required = false) String dateFrom,
            @RequestParam(required = false) String dateTo,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) String username,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String action) {

        try {
            var topUrls = logParsingService.getTopUrlsWithFilters(urlLimit, dateFrom, dateTo,
                    ip, username, status, action);
            var topUsers = logParsingService.getTopUsersWithFilters(userLimit, dateFrom, dateTo,
                    ip, username, status, action);

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "topUrls", Map.of(
                            "data", topUrls,
                            "count", topUrls.size(),
                            "limit", urlLimit
                    ),
                    "topUsers", Map.of(
                            "data", topUsers,
                            "count", topUsers.size(),
                            "limit", userLimit
                    ),
                    "filters", Map.of(
                            "dateFrom", dateFrom != null ? dateFrom : "all",
                            "dateTo", dateTo != null ? dateTo : "all",
                            "ip", ip != null ? ip : "all",
                            "username", username != null ? username : "all",
                            "status", status != null ? status : "all",
                            "action", action != null ? action : "all"
                    )
            ));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of(
                    "success", false,
                    "error", "Ошибка получения комбинированных топов: " + e.getMessage()
            ));
        }
    }

    // Добавляем метод для проверки доступности фильтрованных топов
    @GetMapping("/check-filtered-tops")
    public ResponseEntity<?> checkFilteredTops(
            @RequestParam(required = false) String dateFrom,
            @RequestParam(required = false) String dateTo) {

        try {
            // Проверяем наличие агрегированной статистики для периода
            var stats = logParsingService.getAggregatedStatsForPeriod(dateFrom, dateTo);
            boolean hasPrecalculated = stats != null && !stats.isEmpty();

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "hasPrecalculatedData", hasPrecalculated,
                    "message", hasPrecalculated ?
                            "Используется предварительно рассчитанная статистика" :
                            "Будет рассчитано в реальном времени",
                    "dateFrom", dateFrom != null ? dateFrom : "not specified",
                    "dateTo", dateTo != null ? dateTo : "not specified"
            ));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of(
                    "success", false,
                    "error", "Ошибка проверки: " + e.getMessage()
            ));
        }
    }

    @PostMapping("/check-file")
    public ResponseEntity<?> checkFileExists(@RequestBody Map<String, String> request) {
        try {
            String filePath = request.get("filePath");

            if (filePath == null || filePath.isEmpty()) {
                return ResponseEntity.ok(Map.of("exists", false, "error", "Путь не указан"));
            }

            java.nio.file.Path path = java.nio.file.Paths.get(filePath);
            boolean exists = java.nio.file.Files.exists(path) &&
                    java.nio.file.Files.isRegularFile(path);

            // Сохраняем путь в сессии или памяти для последующего использования
            if (exists) {
                return ResponseEntity.ok(Map.of(
                        "exists", true,
                        "size", java.nio.file.Files.size(path),
                        "fileName", path.getFileName().toString()
                ));
            } else {
                return ResponseEntity.ok(Map.of(
                        "exists", false,
                        "error", "Файл не найден"
                ));
            }

        } catch (Exception e) {
            return ResponseEntity.ok(Map.of(
                    "exists", false,
                    "error", "Ошибка: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/statuses")
    public ResponseEntity<?> getStatuses() {
        try {
            List<Integer> statuses = new ArrayList<>();

            // Используем прямое подключение как в других методах
            String DB_URL = "jdbc:postgresql://localhost:5432/ParserLog";
            String DB_USERNAME = "postgres";
            String DB_PASSWORD = "uthgb123";

            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {
                // Сначала проверяем существование таблицы
                boolean tableExists = false;
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                             "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'log_statuses')")) {
                    if (rs.next()) {
                        tableExists = rs.getBoolean(1);
                    }
                }

                if (tableExists) {
                    // Получаем статусы отсортированные по частоте использования
                    String sql = "SELECT ls.status_code, COUNT(l.id) as usage_count " +
                            "FROM log_statuses ls " +
                            "LEFT JOIN logs l ON ls.status_code = l.status_code " +
                            "GROUP BY ls.status_code " +
                            "ORDER BY usage_count DESC, ls.status_code";

                    try (PreparedStatement ps = conn.prepareStatement(sql);
                         ResultSet rs = ps.executeQuery()) {

                        while (rs.next()) {
                            statuses.add(rs.getInt("status_code"));
                        }
                    }
                } else {
                    // Если таблицы нет, берем из logs напрямую
                    String sql = "SELECT DISTINCT status_code FROM logs " +
                            "WHERE status_code IS NOT NULL AND status_code > 0 " +
                            "ORDER BY status_code ASC";

                    try (PreparedStatement ps = conn.prepareStatement(sql);
                         ResultSet rs = ps.executeQuery()) {

                        while (rs.next()) {
                            statuses.add(rs.getInt("status_code"));
                        }
                    }
                }
            }

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "statuses", statuses,
                    "count", statuses.size()
            ));

        } catch (Exception e) {
            System.err.println("Ошибка получения статусов: " + e.getMessage());
            return ResponseEntity.ok(Map.of(
                    "success", false,
                    "error", "Ошибка получения статусов",
                    "statuses", new ArrayList<>(),
                    "count", 0
            ));
        }
    }

    @GetMapping("/actions")
    public ResponseEntity<?> getActions() {
        try {
            List<String> actions = new ArrayList<>();

            String DB_URL = "jdbc:postgresql://localhost:5432/ParserLog";
            String DB_USERNAME = "postgres";
            String DB_PASSWORD = "uthgb123";

            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {
                // Сначала проверяем существование таблицы
                boolean tableExists = false;
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                             "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'log_actions')")) {
                    if (rs.next()) {
                        tableExists = rs.getBoolean(1);
                    }
                }

                String sql;
                if (tableExists) {
                    // Получаем actions из таблицы
                    sql = "SELECT action FROM log_actions ORDER BY action";
                } else {
                    // Если таблицы нет, берем из logs напрямую
                    sql = "SELECT DISTINCT action FROM logs " +
                            "WHERE action IS NOT NULL AND action != '' " +
                            "ORDER BY action";
                }

                try (PreparedStatement ps = conn.prepareStatement(sql);
                     ResultSet rs = ps.executeQuery()) {

                    while (rs.next()) {
                        String action = rs.getString("action");
                        if (action != null && !action.trim().isEmpty()) {
                            actions.add(action.trim());
                        }
                    }
                }
            }

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "actions", actions,
                    "count", actions.size()
            ));

        } catch (Exception e) {
            System.err.println("Ошибка получения actions: " + e.getMessage());
            return ResponseEntity.ok(Map.of(
                    "success", false,
                    "error", "Ошибка получения actions",
                    "actions", new ArrayList<>(),
                    "count", 0
            ));
        }
    }

    @PostMapping("/cancel-parsing")
    public ResponseEntity<?> cancelParsing() {
        System.out.println("=== ЗАПРОС НА ОТМЕНУ ПАРСИНГА ===");

        try {
            boolean cancelled = logParsingService.cancelParsing();

            if (cancelled) {
                System.out.println("Парсинг успешно отменен");
                return ResponseEntity.ok(Map.of(
                        "success", true,
                        "message", "Парсинг отменен"
                ));
            } else {
                System.out.println("Парсинг не выполняется, отмена невозможна");
                return ResponseEntity.ok(Map.of(
                        "success", false,
                        "error", "Парсинг не выполняется"
                ));
            }

        } catch (Exception e) {
            System.err.println("Ошибка отмены парсинга: " + e.getMessage());
            return ResponseEntity.status(500).body(Map.of(
                    "success", false,
                    "error", "Ошибка отмены парсинга: " + e.getMessage()
            ));
        }
    }
}