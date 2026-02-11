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

        System.out.println("Сервис: запуск парсинга в отдельном потоке");

        // Запускаем парсинг в отдельном потоке
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

        // ФАКТИЧЕСКОЕ ВРЕМЯ
        status.put("parsingDuration", currentStatus.parsingDuration);
        status.put("finalizationDuration", currentStatus.finalizationDuration);
        status.put("indexingDuration", currentStatus.indexingDuration);
        status.put("statisticsDuration", currentStatus.statisticsDuration);

        // КОЭФФИЦИЕНТЫ
        status.put("finalizationFactor", currentStatus.finalizationFactor);
        status.put("indexingFactor", currentStatus.indexingFactor);
        status.put("statisticsFactor", currentStatus.statisticsFactor);

        // РАСЧЕТ ОСТАВШЕГОСЯ ВРЕМЕНИ
        status.put("estimatedTotalTime", currentStatus.estimatedTotalTime);
        status.put("estimatedParsingTime", currentStatus.estimatedParsingTime);
        status.put("estimatedFinalizationTime", currentStatus.estimatedFinalizationTime);
        status.put("estimatedIndexingTime", currentStatus.estimatedIndexingTime);
        status.put("estimatedStatisticsTime", currentStatus.estimatedStatisticsTime);


        if (currentStatus.isParsing && currentStatus.startTime > 0) {
            long elapsed = System.currentTimeMillis() - currentStatus.startTime;

            // Общее оставшееся время
            if (currentStatus.estimatedTotalTime > 0) {
                long remainingTotal = Math.max(0, currentStatus.estimatedTotalTime - elapsed);
                status.put("remainingTotal", formatDuration(remainingTotal));
            }

            // Оставшееся время текущего этапа
            String stage = currentStatus.stageName;
            long stageRemaining = 0;

            if (stage.contains("Парсинг")) {
                stageRemaining = Math.max(0, currentStatus.estimatedParsingTime -
                        (elapsed - currentStatus.parsingDuration));
            } else if (stage.contains("Финализация")) {
                stageRemaining = Math.max(0, currentStatus.estimatedFinalizationTime -
                        (elapsed - currentStatus.parsingDuration));
            } else if (stage.contains("Индексация")) {
                stageRemaining = Math.max(0, currentStatus.estimatedIndexingTime -
                        (elapsed - currentStatus.parsingDuration - currentStatus.finalizationDuration));
            } else if (stage.contains("Статистика")) {
                stageRemaining = Math.max(0, currentStatus.estimatedStatisticsTime -
                        (elapsed - currentStatus.parsingDuration - currentStatus.finalizationDuration -
                                currentStatus.indexingDuration));
            }

            status.put("remainingStage", formatDuration(stageRemaining));
        }
        return status;
    }

    private String calculateRemainingTime(ParsingStatus status, long elapsed) {
        if (status.stageName.contains("Парсинг") && status.total > 0 && status.processed > 0) {
            double progress = (double) status.processed / status.total;
            if (progress > 0.01) {
                long estimatedTotal = (long)(elapsed / progress);
                long remaining = Math.max(0, estimatedTotal - elapsed);
                return formatDuration(remaining);
            }
        } else if (status.stageName.contains("Финализация") && status.estimatedFinalizationTime > 0) {
            long stageElapsed = elapsed - status.parsingDuration;
            long remaining = Math.max(0, status.estimatedFinalizationTime - stageElapsed);
            return formatDuration(remaining);
        } else if (status.stageName.contains("Индексация") && status.estimatedIndexingTime > 0) {
            long stageElapsed = elapsed - status.parsingDuration - status.finalizationDuration;
            long remaining = Math.max(0, status.estimatedIndexingTime - stageElapsed);
            return formatDuration(remaining);
        } else if (status.stageName.contains("Статистика") && status.estimatedStatisticsTime > 0) {
            long stageElapsed = elapsed - status.parsingDuration - status.finalizationDuration - status.indexingDuration;
            long remaining = Math.max(0, status.estimatedStatisticsTime - stageElapsed);
            return formatDuration(remaining);
        }
        return "расчет...";
    }

    private String formatDuration(long ms) {
        if (ms < 0) return "0 сек";
        if (ms < 60000) {
            return "~" + (ms / 1000) + " сек";
        } else {
            return "~" + (ms / 60000) + " мин " + ((ms % 60000) / 1000) + " сек";
        }
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