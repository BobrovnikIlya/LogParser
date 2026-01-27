package com.work.LogParser.service;

import com.work.LogParser.model.ParsingStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import com.work.LogParser.service.LogDataRepository;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
public class LogParsingService {
    @Autowired
    private LogFileParser logFileParser;

    @Autowired
    private LogDataRepository logDataRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile ParsingStatus currentStatus = new ParsingStatus();
    private Future<?> parsingTask;

    public boolean startParsing(String filePath) {
        System.out.println("Сервис: попытка запуска парсинга для файла: " + filePath);

        if (currentStatus.isParsing) {
            System.out.println("Сервис: парсинг уже выполняется, отказ");
            return false;
        }

        // Сбрасываем статус
        currentStatus = new ParsingStatus();
        currentStatus.isParsing = true;
        currentStatus.status = "Начало парсинга";
        currentStatus.filePath = filePath;
        currentStatus.startTime = System.currentTimeMillis();

        System.out.println("Сервис: запуск парсинга в отдельном потоке");

        // Запускаем парсинг в отдельном потоке
        parsingTask = executor.submit(() -> {
            try {
                logFileParser.parseWithOriginalCode(filePath, currentStatus);
            } catch (Exception e) {
                System.err.println("Сервис: ошибка в потоке парсинга: " + e.getMessage());
                e.printStackTrace();
                currentStatus.isParsing = false;
                currentStatus.status = "❌ Ошибка: " + e.getMessage();
            }
        });

        return true;
    }

    public Map<String, Object> getParsingStatus() {
        System.out.println("Сервис: запрос статуса парсинга");
        System.out.println("Текущий статус: isParsing=" + currentStatus.isParsing +
                ", progress=" + currentStatus.progress +
                ", status=" + currentStatus.status);

        Map<String, Object> status = new HashMap<>();
        status.put("success", true);
        status.put("isParsing", currentStatus.isParsing);
        status.put("status", currentStatus.status);
        status.put("progress", currentStatus.progress);
        status.put("processed", currentStatus.processed);
        status.put("total", currentStatus.total);

        // Рассчитываем оставшееся время если идет парсинг
        if (currentStatus.isParsing && currentStatus.progress > 0 && currentStatus.startTime > 0) {
            long elapsed = System.currentTimeMillis() - currentStatus.startTime;
            double estimatedTotal = elapsed / (currentStatus.progress / 100.0);
            long remaining = (long) ((estimatedTotal - elapsed) / 1000);

            if (remaining < 60) {
                status.put("remaining", "~" + remaining + " сек");
            } else {
                status.put("remaining", "~" + (remaining / 60) + " мин");
            }

            status.put("elapsed", elapsed / 1000); // в секундах
        }

        status.put("filePath", currentStatus.filePath);
        return status;
    }

    public Map<String, Object> getLogsWithStats(int page, int size,
                                                String dateFrom, String dateTo,
                                                String clientIp, String username,
                                                String status, String search, String action) {
        return logDataRepository.getLogsWithStats(page, size, dateFrom, dateTo, clientIp, username, status, search, action);
    }

    public boolean hasDataInDatabase() {
        return logDataRepository.hasDataInDatabase();
    }

    public long getLogCount() {
        return logDataRepository.getLogCount();
    }

    public List<Map<String, Object>> getTopUrls(int limit) {
        return logDataRepository.getTopUrls(limit);
    }

    public List<Map<String, Object>> getTopUsers(int limit) {
        return logDataRepository.getTopUsers(limit);
    }

    public List<Integer> getAvailableStatuses() {
        return logDataRepository.getAvailableStatuses();
    }

    public List<String> getAvailableActions() {
        return logDataRepository.getAvailableActions();
    }
}