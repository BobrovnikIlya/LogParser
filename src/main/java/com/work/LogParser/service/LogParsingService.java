package com.work.LogParser.service;

import com.work.LogParser.model.ParsingStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import com.work.LogParser.repository.LogDataRepository;

import java.time.LocalDateTime;
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
    private PrecalculatedTopService precalculatedTopService;

    @Autowired
    private LogDataRepository logDataRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private FilterCacheService filterCacheService;

    @Autowired
    private AggregatedStatsService aggregatedStatsService;

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
        currentStatus.isCancelled = false;
        currentStatus.startTime = System.currentTimeMillis();

        // ИНИЦИАЛИЗИРУЕМ НОВЫЕ ПОЛЯ
        currentStatus.parsingSpeed = 0;
        currentStatus.parsingStageStartTime = 0;
        currentStatus.lastProgressUpdateTime = System.currentTimeMillis();
        currentStatus.lastProcessedCount = 0;

        System.out.println("Сервис: запуск парсинга в отдельном потоке");

        parsingTask = executor.submit(() -> {
            try {
                logFileParser.parseWithHybridCopy(filePath, currentStatus);
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

        Map<String, Object> status = new HashMap<>();
        status.put("success", true);
        status.put("isParsing", currentStatus.isParsing);
        status.put("status", currentStatus.status);
        status.put("progress", currentStatus.progress);
        status.put("stageProgress", currentStatus.stageProgress);
        status.put("stageName", currentStatus.stageName);
        status.put("processed", currentStatus.processed);
        status.put("total", currentStatus.total);
        status.put("filePath", currentStatus.filePath);

        // Добавляем временные оценки
        status.put("parsingDuration", currentStatus.parsingDuration);
        status.put("estimatedFinalizationTime", currentStatus.estimatedFinalizationTime);
        status.put("estimatedIndexingTime", currentStatus.estimatedIndexingTime);
        status.put("estimatedStatisticsTime", currentStatus.estimatedStatisticsTime);

        // РАСЧЕТ ОСТАВШЕГОСЯ ВРЕМЕНИ
        if (currentStatus.isParsing && currentStatus.startTime > 0) {
            long elapsed = System.currentTimeMillis() - currentStatus.startTime;
            status.put("elapsed", elapsed / 1000);

            String stage = currentStatus.stageName;

            // ЭТАП ПАРСИНГА - ИСПРАВЛЕННАЯ ВЕРСИЯ
            if (stage.contains("Парсинг") || stage.contains("🚀 Парсинг")) {
                if (currentStatus.total > 0 && currentStatus.processed > 0) {
                    long now = System.currentTimeMillis();

                    // Обновляем скорость каждые 2 секунды
                    if (now - currentStatus.lastProgressUpdateTime > 2000) {
                        long processedDelta = currentStatus.processed - currentStatus.lastProcessedCount;
                        long timeDelta = now - currentStatus.lastProgressUpdateTime;

                        if (timeDelta > 0 && processedDelta > 0) {
                            double instantSpeed = (processedDelta * 1000.0) / timeDelta;
                            // Сглаживание
                            if (currentStatus.parsingSpeed == 0) {
                                currentStatus.parsingSpeed = instantSpeed;
                            } else {
                                currentStatus.parsingSpeed = currentStatus.parsingSpeed * 0.7 + instantSpeed * 0.3;
                            }
                        }

                        currentStatus.lastProgressUpdateTime = now;
                        currentStatus.lastProcessedCount = currentStatus.processed;
                    }

                    // Используем сохраненную скорость или вычисляем среднюю
                    double speed = currentStatus.parsingSpeed;
                    if (speed <= 0) {
                        long elapsedParsing = now - currentStatus.parsingStageStartTime;
                        if (elapsedParsing > 0) {
                            speed = (currentStatus.processed * 1000.0) / elapsedParsing;
                        }
                    }

                    // Расчет оставшегося времени
                    if (speed > 0) {
                        long remainingLines = currentStatus.total - currentStatus.processed;
                        long remainingSeconds = (long) (remainingLines / speed);

                        if (remainingSeconds < 60) {
                            status.put("remaining", "~" + remainingSeconds + " сек");
                        } else {
                            status.put("remaining", "~" + (remainingSeconds / 60) + " мин " +
                                    (remainingSeconds % 60) + " сек");
                        }
                        status.put("processingSpeed", String.format("%.0f", speed) + " строк/сек");
                    } else {
                        status.put("remaining", "расчет...");
                    }
                } else {
                    status.put("remaining", "подготовка...");
                }
            }
            // ЭТАП ФИНАЛИЗАЦИИ
            else if (stage.contains("Финализация")) {
                if (currentStatus.estimatedFinalizationTime > 0 && currentStatus.stageProgress < 100) {
                    long stageElapsed = elapsed - currentStatus.parsingDuration;
                    long stageRemaining = Math.max(0,
                            (long)(currentStatus.estimatedFinalizationTime * (100 - currentStatus.stageProgress) / 100));

                    if (stageRemaining < 60000) {
                        status.put("remaining", "~" + (stageRemaining / 1000) + " сек");
                    } else {
                        status.put("remaining", "~" + (stageRemaining / 60000) + " мин");
                    }
                }
            }
            // ЭТАП ИНДЕКСАЦИИ
            else if (stage.contains("Индексация")) {
                if (currentStatus.estimatedIndexingTime > 0 && currentStatus.stageProgress < 100) {
                    long stageElapsed = elapsed - currentStatus.parsingDuration - currentStatus.estimatedFinalizationTime;
                    long stageRemaining = Math.max(0,
                            (long)(currentStatus.estimatedIndexingTime * (100 - currentStatus.stageProgress) / 100));

                    if (stageRemaining < 60000) {
                        status.put("remaining", "~" + (stageRemaining / 1000) + " сек");
                    } else {
                        status.put("remaining", "~" + (stageRemaining / 60000) + " мин");
                    }
                }
            }
            // ЭТАП СТАТИСТИКИ
            else if (stage.contains("Статистика")) {
                if (currentStatus.estimatedStatisticsTime > 0 && currentStatus.stageProgress < 100) {
                    long stageElapsed = elapsed - currentStatus.parsingDuration
                            - currentStatus.estimatedFinalizationTime
                            - currentStatus.estimatedIndexingTime;
                    long stageRemaining = Math.max(0,
                            (long)(currentStatus.estimatedStatisticsTime * (100 - currentStatus.stageProgress) / 100));

                    if (stageRemaining < 60000) {
                        status.put("remaining", "~" + (stageRemaining / 1000) + " сек");
                    } else {
                        status.put("remaining", "~" + (stageRemaining / 60000) + " мин");
                    }
                }
            }
        }

        return status;
    }

    public Map<String, Object> getLogsWithStats(int page, int size,
                                                String dateFrom, String dateTo,
                                                String clientIp, String username,
                                                String status, String search, String action) {

        String cacheKey = filterCacheService.generateCacheKey(dateFrom, dateTo, clientIp,
                username, status, action);

        boolean areFiltersEmpty = filterCacheService.areFiltersEmpty(dateFrom, dateTo, clientIp,
                username, status, action);

        return filterCacheService.getCachedFilterResults(cacheKey, () -> {
            return logDataRepository.getLogsWithStats(page, size, dateFrom, dateTo,
                    clientIp, username, status, search, action);
        }, dateFrom, dateTo, clientIp, username, status, action);
    }

    public List<Map<String, Object>> getTopUrlsWithFilters(int limit,
                                                           String dateFrom, String dateTo,
                                                           String clientIp, String username,
                                                           String status, String action) {

        // Проверяем пустые ли фильтры
        boolean areFiltersEmpty = filterCacheService.areFiltersEmpty(dateFrom, dateTo, clientIp,
                username, status, action);

        if (areFiltersEmpty) {
            // Используем прерассчитанные данные из таблицы
            List<Map<String, Object>> precalculated = precalculatedTopService.getPrecalculatedTopUrls(limit);
            if (precalculated != null && !precalculated.isEmpty()) {
                System.out.println("📊 Используем прерассчитанные топ URL (дефолт)");
                return precalculated;
            }
        }

        // Для непустых фильтров используем кэш и динамический расчет
        String cacheKey = filterCacheService.generateTopCacheKey(dateFrom, dateTo, clientIp,
                username, status, action, "urls", limit);

        return filterCacheService.getCachedTopUrls(cacheKey, () -> {
            return logDataRepository.getTopUrlsWithFilters(limit, dateFrom, dateTo,
                    clientIp, username, status, action);
        }, areFiltersEmpty);
    }

    public List<Map<String, Object>> getTopUsersWithFilters(int limit,
                                                            String dateFrom, String dateTo,
                                                            String clientIp, String username,
                                                            String status, String action) {

        // Проверяем пустые ли фильтры
        boolean areFiltersEmpty = filterCacheService.areFiltersEmpty(dateFrom, dateTo, clientIp,
                username, status, action);

        if (areFiltersEmpty) {
            // Используем прерассчитанные данные из таблицы
            List<Map<String, Object>> precalculated = precalculatedTopService.getPrecalculatedTopUsers(limit);
            if (precalculated != null && !precalculated.isEmpty()) {
                System.out.println("👥 Используем прерассчитанные топ пользователей (дефолт)");
                return precalculated;
            }
        }

        // Для непустых фильтров используем кэш и динамический расчет
        String cacheKey = filterCacheService.generateTopCacheKey(dateFrom, dateTo, clientIp,
                username, status, action, "users", limit);

        return filterCacheService.getCachedTopUsers(cacheKey, () -> {
            return logDataRepository.getTopUsersWithFilters(limit, dateFrom, dateTo,
                    clientIp, username, status, action);
        }, areFiltersEmpty);
    }

    public void updatePrecalculatedTops() {
        precalculatedTopService.updatePrecalculatedTops();
    }

    public boolean hasDataInDatabase() {
        return logDataRepository.hasDataInDatabase();
    }

    public long getLogCount() {
        return logDataRepository.getLogCount();
    }

    public List<Map<String, Object>> getTopUrls(int limit) {
        // Ограничиваем максимальный лимит 100
        int actualLimit = Math.min(limit, 100);
        return getTopUrlsWithFilters(actualLimit, null, null, null, null, null, null);
    }

    public List<Map<String, Object>> getTopUsers(int limit) {
        // Ограничиваем максимальный лимит 10
        int actualLimit = Math.min(limit, 10);
        return getTopUsersWithFilters(actualLimit, null, null, null, null, null, null);
    }

    public List<Integer> getAvailableStatuses() {
        return logDataRepository.getAvailableStatuses();
    }

    public List<String> getAvailableActions() {
        return logDataRepository.getAvailableActions();
    }

    public boolean cancelParsing() {
        if (!currentStatus.isParsing) {
            return false;
        }

        // Устанавливаем флаг отмены
        currentStatus.isCancelled = true;

        // Пытаемся прервать задачу
        if (parsingTask != null && !parsingTask.isDone()) {
            parsingTask.cancel(true);
        }

        System.out.println("Парсинг отменен по запросу пользователя");
        return true;
    }

    public Map<String, Object> getAggregatedStatsForPeriod(String dateFrom, String dateTo) {
        try {
            // Конвертируем строки в LocalDateTime
            LocalDateTime from = null;
            LocalDateTime to = null;

            if (dateFrom != null && !dateFrom.isEmpty()) {
                from = LocalDateTime.parse(dateFrom.replace(" ", "T"));
            }

            if (dateTo != null && !dateTo.isEmpty()) {
                to = LocalDateTime.parse(dateTo.replace(" ", "T"));
            }

            // Получаем агрегированную статистику
            return aggregatedStatsService.getAggregatedStats(from, to);
        } catch (Exception e) {
            System.err.println("Ошибка получения агрегированной статистики: " + e.getMessage());
            return null;
        }
    }
}