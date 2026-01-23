package com.work.LogParser.controller;

import com.work.LogParser.service.LogParsingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

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

    @GetMapping("/top-urls")
    public ResponseEntity<?> getTopUrls() {
        try {
            var topUrls = logParsingService.getTopUrls(100);
            return ResponseEntity.ok(Map.of("success", true, "data", topUrls));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of(
                    "success", false, "error", "Ошибка: " + e.getMessage()
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

    @GetMapping("/top-users")
    public ResponseEntity<?> getTopUsers() {
        try {
            var topUsers = logParsingService.getTopUsers(10);
            return ResponseEntity.ok(Map.of("success", true, "data", topUsers));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of(
                    "success", false, "error", "Ошибка: " + e.getMessage()
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
}