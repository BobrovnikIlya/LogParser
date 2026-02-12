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
        status.put("isCancelled", currentStatus.isCancelled);

        // Добавляем временные оценки
        status.put("parsingDuration", currentStatus.parsingDuration);
        status.put("estimatedFinalizationTime", currentStatus.estimatedFinalizationTime);
        status.put("estimatedIndexingTime", currentStatus.estimatedIndexingTime);
        status.put("estimatedStatisticsTime", currentStatus.estimatedStatisticsTime);

        // Добавляем фактические времена этапов (новые поля)
        status.put("actualParsingTime", currentStatus.actualParsingTime);
        status.put("actualFinalizationTime", currentStatus.actualFinalizationTime);
        status.put("actualIndexingTime", currentStatus.actualIndexingTime);
        status.put("actualStatisticsTime", currentStatus.actualStatisticsTime);

        // Флаги завершения этапов
        status.put("parsingCompleted", currentStatus.parsingCompleted);
        status.put("finalizationCompleted", currentStatus.finalizationCompleted);
        status.put("indexingCompleted", currentStatus.indexingCompleted);
        status.put("statisticsCompleted", currentStatus.statisticsCompleted);

        // РАСЧЕТ ВРЕМЕНИ
        if (currentStatus.isParsing && currentStatus.startTime > 0) {
            long now = System.currentTimeMillis();
            long elapsed = now - currentStatus.startTime;
            status.put("elapsed", elapsed / 1000);

            String stage = currentStatus.stageName;

            // === ЭТАП ПАРСИНГА ===
            if (stage.contains("Парсинг") || stage.contains("🚀 Парсинг")) {
                calculateParsingStageStatus(status, now, elapsed);
            }
            // === ЭТАП ФИНАЛИЗАЦИИ ===
            else if (stage.contains("Финализация") || stage.contains("🗃️ Финализация")) {
                calculateFinalizationStageStatus(status, now, elapsed);
            }
            // === ЭТАП ИНДЕКСАЦИИ ===
            else if (stage.contains("Индексация") || stage.contains("📈 Создание индексов")) {
                calculateIndexingStageStatus(status, now, elapsed);
            }
            // === ЭТАП СТАТИСТИКИ ===
            else if (stage.contains("Статистика") || stage.contains("📊 Обновление статистики")) {
                calculateStatisticsStageStatus(status, now, elapsed);
            }
        }

        return status;
    }

    /**
     * Расчет статуса для этапа парсинга
     */
    private void calculateParsingStageStatus(Map<String, Object> status, long now, long elapsed) {
        status.put("stageType", "parsing");

        if (currentStatus.total > 0 && currentStatus.processed > 0) {
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

                status.put("remaining", formatRemainingTime(remainingSeconds * 1000));
                status.put("remainingSeconds", remainingSeconds);
                status.put("processingSpeed", String.format("%.0f", speed) + " строк/сек");
                status.put("processingSpeedValue", speed);
            } else {
                status.put("remaining", "расчет...");
                status.put("remainingSeconds", -1);
            }

            // Прогресс этапа
            status.put("stageProgress", currentStatus.stageProgress);
            status.put("stageProgressValue", currentStatus.stageProgress / 100.0);

            // Общий прогресс
            status.put("progress", currentStatus.progress);
        } else {
            status.put("remaining", "подготовка...");
            status.put("remainingSeconds", -1);
        }
    }

    /**
     * Расчет статуса для этапа финализации
     */
    private void calculateFinalizationStageStatus(Map<String, Object> status, long now, long elapsed) {
        status.put("stageType", "finalization");

        // Время, затраченное на предыдущие этапы (ИСПОЛЬЗУЕМ ФАКТИЧЕСКОЕ ВРЕМЯ)
        long previousTimeSpent = 0;

        if (currentStatus.parsingCompleted) {
            previousTimeSpent += currentStatus.actualParsingTime;
        } else {
            // Если парсинг еще не завершен (аварийная ситуация), используем оценку
            previousTimeSpent += currentStatus.parsingDuration > 0 ?
                    currentStatus.parsingDuration : (elapsed - currentStatus.parsingStageStartTime);
        }

        // Время, затраченное на текущий этап
        long stageElapsed = currentStatus.stageStartTime > 0 ?
                now - currentStatus.stageStartTime : elapsed - previousTimeSpent;

        status.put("stageElapsed", stageElapsed / 1000);

        // Прогресс этапа
        int stageProgress = (int) currentStatus.stageProgress;
        status.put("stageProgress", stageProgress);
        status.put("stageProgressValue", stageProgress / 100.0);

        // Расчет оставшегося времени
        if (stageProgress < 100 && stageProgress > 0) {
            long remainingTime = calculateRemainingTimeForStage(
                    stageElapsed,
                    stageProgress,
                    currentStatus.estimatedFinalizationTime
            );

            status.put("remaining", formatRemainingTime(remainingTime));
            status.put("remainingSeconds", remainingTime / 1000);

            // Расчет скорости выполнения этапа
            if (stageElapsed > 0 && stageProgress > 0) {
                double stageSpeed = stageProgress / (stageElapsed / 1000.0);
                status.put("stageSpeed", String.format("%.1f", stageSpeed) + "%/сек");
            }
        } else {
            status.put("remaining", "финализация...");
            status.put("remainingSeconds", -1);
        }

        // Общий прогресс
        status.put("progress", currentStatus.progress);
    }

    /**
     * Расчет статуса для этапа индексации
     */
    private void calculateIndexingStageStatus(Map<String, Object> status, long now, long elapsed) {
        status.put("stageType", "indexing");

        // Время, затраченное на предыдущие этапы (ТОЛЬКО ФАКТИЧЕСКОЕ ВРЕМЯ)
        long previousTimeSpent = 0;

        // Парсинг - всегда должно быть фактическое время
        if (currentStatus.parsingCompleted) {
            previousTimeSpent += currentStatus.actualParsingTime;
        } else {
            previousTimeSpent += currentStatus.parsingDuration;
        }

        // Финализация - используем фактическое время, если завершена
        if (currentStatus.finalizationCompleted) {
            previousTimeSpent += currentStatus.actualFinalizationTime;
        } else {
            // Если финализация не завершена (аварийная ситуация), используем оценку
            previousTimeSpent += currentStatus.estimatedFinalizationTime;
        }

        // Время, затраченное на текущий этап
        long stageElapsed = currentStatus.stageStartTime > 0 ?
                now - currentStatus.stageStartTime : Math.max(0, elapsed - previousTimeSpent);

        status.put("stageElapsed", stageElapsed / 1000);

        // Прогресс этапа
        int stageProgress = (int) currentStatus.stageProgress;
        status.put("stageProgress", stageProgress);
        status.put("stageProgressValue", stageProgress / 100.0);

        // Информация о созданных индексах
        status.put("indexesCreated", currentStatus.indexesCreated);
        status.put("totalIndexes", currentStatus.totalIndexes);

        // Расчет оставшегося времени
        if (stageProgress < 100 && stageProgress > 0) {
            long remainingTime = calculateRemainingTimeForStage(
                    stageElapsed,
                    stageProgress,
                    currentStatus.estimatedIndexingTime
            );

            status.put("remaining", formatRemainingTime(remainingTime));
            status.put("remainingSeconds", remainingTime / 1000);
        } else {
            status.put("remaining", "индексация...");
            status.put("remainingSeconds", -1);
        }

        // Общий прогресс
        status.put("progress", currentStatus.progress);
    }

    /**
     * Расчет статуса для этапа статистики
     */
    private void calculateStatisticsStageStatus(Map<String, Object> status, long now, long elapsed) {
        status.put("stageType", "statistics");

        // Время, затраченное на предыдущие этапы (ТОЛЬКО ФАКТИЧЕСКОЕ ВРЕМЯ)
        long previousTimeSpent = 0;

        // Парсинг - фактическое время
        if (currentStatus.parsingCompleted) {
            previousTimeSpent += currentStatus.actualParsingTime;
        } else {
            previousTimeSpent += currentStatus.parsingDuration;
        }

        // Финализация - фактическое время, если завершена
        if (currentStatus.finalizationCompleted) {
            previousTimeSpent += currentStatus.actualFinalizationTime;
        } else {
            previousTimeSpent += currentStatus.estimatedFinalizationTime;
        }

        // Индексация - фактическое время, если завершена
        if (currentStatus.indexingCompleted) {
            previousTimeSpent += currentStatus.actualIndexingTime;
        } else {
            previousTimeSpent += currentStatus.estimatedIndexingTime;
        }

        // Время, затраченное на текущий этап
        long stageElapsed = currentStatus.stageStartTime > 0 ?
                now - currentStatus.stageStartTime : Math.max(0, elapsed - previousTimeSpent);

        status.put("stageElapsed", stageElapsed / 1000);

        // Прогресс этапа
        int stageProgress = (int) currentStatus.stageProgress;
        status.put("stageProgress", stageProgress);
        status.put("stageProgressValue", stageProgress / 100.0);

        // Расчет оставшегося времени
        if (stageProgress < 100 && stageProgress > 0) {
            long remainingTime = calculateRemainingTimeForStage(
                    stageElapsed,
                    stageProgress,
                    currentStatus.estimatedStatisticsTime
            );

            status.put("remaining", formatRemainingTime(remainingTime));
            status.put("remainingSeconds", remainingTime / 1000);
        } else {
            status.put("remaining", "обновление статистики...");
            status.put("remainingSeconds", -1);
        }

        // Общий прогресс
        status.put("progress", currentStatus.progress);
    }

    /**
     * Расчет оставшегося времени для этапа с адаптивной коррекцией
     */
    private long calculateRemainingTimeForStage(long stageElapsed, int stageProgress, long estimatedTime) {
        if (stageProgress <= 0) {
            return estimatedTime;
        }

        if (stageProgress >= 100) {
            return 0;
        }

        // Базовый расчет на основе оценки
        long remainingByEstimate = (long) (estimatedTime * (100 - stageProgress) / 100.0);

        // Если прошло достаточно времени для точного расчета
        if (stageElapsed > 5000 && stageProgress > 5) {
            // Расчет на основе фактической скорости
            double progressPerMs = stageProgress / (double) stageElapsed;
            long remainingByActual = (long) ((100 - stageProgress) / progressPerMs);

            // Адаптивное взвешивание: чем больше прогресс, тем больше доверия к фактической скорости
            double actualWeight = Math.min(0.9, stageProgress / 100.0);
            double estimateWeight = 1.0 - actualWeight;

            return (long) (remainingByActual * actualWeight + remainingByEstimate * estimateWeight);
        }

        return remainingByEstimate;
    }

    /**
     * Форматирование оставшегося времени в человекочитаемый формат
     */
    private String formatRemainingTime(long milliseconds) {
        if (milliseconds <= 0) {
            return "менее секунды";
        }

        long seconds = milliseconds / 1000;

        if (seconds < 60) {
            return "~" + seconds + " сек";
        } else if (seconds < 3600) {
            long minutes = seconds / 60;
            long remainingSeconds = seconds % 60;
            return "~" + minutes + " мин " + remainingSeconds + " сек";
        } else {
            long hours = seconds / 3600;
            long minutes = (seconds % 3600) / 60;
            return "~" + hours + " ч " + minutes + " мин";
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