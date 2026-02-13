package com.work.LogParser.service;

import com.work.LogParser.config.DatabaseConfig;
import com.work.LogParser.model.ParsingStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import com.work.LogParser.repository.LogDataRepository;

import java.sql.*;
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
        Map<String, Object> status = new HashMap<>();

        try {
            status.put("success", true);
            status.put("isParsing", currentStatus.isParsing);
            status.put("status", currentStatus.status != null ? currentStatus.status : "");
            status.put("progress", currentStatus.progress);
            status.put("stageProgress", currentStatus.stageProgress);
            status.put("stageName", currentStatus.stageName != null ? currentStatus.stageName : "");
            status.put("processed", currentStatus.processed);
            status.put("total", currentStatus.total);
            status.put("filePath", currentStatus.filePath != null ? currentStatus.filePath : "");
            status.put("isCancelled", currentStatus.isCancelled);

            if (currentStatus.isCancelled) {
                status.put("estimatedTimeRemaining", 0);
                status.put("remaining", "отменено");
                status.put("remainingSeconds", 0);
                status.put("elapsed", 0);
                status.put("elapsedFormatted", "0 сек");
            } else {
                // ===== РАСЧЕТ ОБЩЕГО ОСТАВШЕГОСЯ ВРЕМЕНИ =====
                if (currentStatus.isParsing && currentStatus.startTime > 0) {
                    // Вызываем исправленную функцию расчета
                    long totalRemaining = calculateTotalRemainingTime(currentStatus);
                    currentStatus.estimatedTimeRemaining = totalRemaining;
                    status.put("estimatedTimeRemaining", totalRemaining);

                    // Добавляем оценки времени этапов
                    status.put("estimatedFinalizationTime", currentStatus.estimatedFinalizationTime);
                    status.put("estimatedIndexingTime", currentStatus.estimatedIndexingTime);
                    status.put("estimatedStatisticsTime", currentStatus.estimatedStatisticsTime);

                    // Прошедшее время
                    long elapsed = System.currentTimeMillis() - currentStatus.startTime;
                    status.put("elapsed", elapsed);
                    status.put("elapsedFormatted", formatRemainingTime(elapsed));
                } else {
                    status.put("estimatedTimeRemaining", 0);
                    status.put("estimatedFinalizationTime", 0);
                    status.put("estimatedIndexingTime", 0);
                    status.put("estimatedStatisticsTime", 0);
                    status.put("elapsed", 0);
                    status.put("elapsedFormatted", "0 сек");
                }

                // ===== ОСТАВШЕЕСЯ ВРЕМЯ ТЕКУЩЕГО ЭТАПА =====
                if (currentStatus.isParsing) {
                    Map<String, Object> stageStatus = calculateCurrentStageRemaining(currentStatus);
                    status.put("remainingSeconds", stageStatus.get("remainingSeconds"));
                    status.put("remaining", stageStatus.get("remaining"));
                } else {
                    status.put("remainingSeconds", -1);
                    status.put("remaining", "завершено");
                }

                // ===== ФАКТИЧЕСКИЕ ВРЕМЕНА ЭТАПОВ =====
                status.put("actualParsingTime", currentStatus.actualParsingTime);
                status.put("actualFinalizationTime", currentStatus.actualFinalizationTime);
                status.put("actualIndexingTime", currentStatus.actualIndexingTime);
                status.put("actualStatisticsTime", currentStatus.actualStatisticsTime);

                // ===== ФЛАГИ ЗАВЕРШЕНИЯ =====
                status.put("parsingCompleted", currentStatus.parsingCompleted);
                status.put("finalizationCompleted", currentStatus.finalizationCompleted);
                status.put("indexingCompleted", currentStatus.indexingCompleted);
                status.put("statisticsCompleted", currentStatus.statisticsCompleted);

                // ===== ИНФОРМАЦИЯ О СКОРОСТИ =====
                status.put("parsingSpeed", currentStatus.parsingSpeed);
            }

        } catch (Exception e) {
            // При любой ошибке возвращаем безопасный статус
            status.put("success", false);
            status.put("error", e.getMessage());
            status.put("isParsing", false);
            status.put("isCancelled", currentStatus.isCancelled);
            status.put("estimatedTimeRemaining", 0);
        }

        return status;
    }

    private long calculateTotalRemainingTime(ParsingStatus status) {
        // Защита от null и некорректного состояния
        if (status == null || !status.isParsing) {
            return 0;
        }

        long totalRemaining = 0;
        long now = System.currentTimeMillis();
        String stage = status.stageName != null ? status.stageName : "";

        // Константы весов этапов
        final double PARSING_WEIGHT = 386.5;
        final double FINALIZATION_WEIGHT = 450.0;
        final double INDEXING_WEIGHT = 220.0;
        final double STATISTICS_WEIGHT = 170.0;

        // ===== 1. ОПРЕДЕЛЯЕМ БАЗОВОЕ ВРЕМЯ ПАРСИНГА =====
        long baseParsingTime = 60000; // 1 минута по умолчанию

        // Если парсинг завершен - используем фактическое время
        if (status.actualParsingTime > 0) {
            baseParsingTime = status.actualParsingTime;
        }
        // Если парсинг в процессе - рассчитываем на основе скорости
        else if (status.parsingSpeed > 0.001 && status.total > 0) {
            double speed = status.parsingSpeed;
            // Защита от слишком маленькой скорости
            if (speed < 1) speed = 1000; // минимум 1000 строк/сек

            long totalLines = Math.max(1, status.total);
            baseParsingTime = (long) ((totalLines / speed) * 1000);

            // Ограничиваем разумными пределами (от 1 секунды до 24 часов)
            baseParsingTime = Math.max(1000, Math.min(baseParsingTime, 24 * 60 * 60 * 1000));
        }
        // Если есть длительность парсинга
        else if (status.parsingDuration > 0) {
            baseParsingTime = status.parsingDuration;
        }

        // ===== 2. РАССЧИТЫВАЕМ ВРЕМЯ ЭТАПОВ =====
        long estimatedFinalizationTime = (long) (baseParsingTime * (FINALIZATION_WEIGHT / PARSING_WEIGHT));
        long estimatedIndexingTime = (long) (baseParsingTime * (INDEXING_WEIGHT / PARSING_WEIGHT));
        long estimatedStatisticsTime = (long) (baseParsingTime * (STATISTICS_WEIGHT / PARSING_WEIGHT));

        // Ограничиваем разумными пределами
        estimatedFinalizationTime = Math.min(estimatedFinalizationTime, 30 * 60 * 1000); // макс 30 минут
        estimatedIndexingTime = Math.min(estimatedIndexingTime, 30 * 60 * 1000);
        estimatedStatisticsTime = Math.min(estimatedStatisticsTime, 30 * 60 * 1000);

        // ===== 3. СОХРАНЯЕМ ОЦЕНКИ В СТАТУС =====
        status.estimatedFinalizationTime = estimatedFinalizationTime;
        status.estimatedIndexingTime = estimatedIndexingTime;
        status.estimatedStatisticsTime = estimatedStatisticsTime;

        // ===== 4. РАСЧЕТ ОСТАВШЕГОСЯ ВРЕМЕНИ ТОЛЬКО ДЛЯ НЕЗАВЕРШЕННЫХ ЭТАПОВ =====

        // ЭТАП ПАРСИНГА
        if (!status.parsingCompleted) {
            if (stage.contains("Подсчет строк") || stage.contains("📊 Подсчет")) {
                // Этап подсчета строк - быстрая оценка
                totalRemaining += 5000; // 5 секунд на подсчет
            } else if (status.total > 0 && status.processed > 0 && status.parsingSpeed > 0.001) {
                // Нормальный парсинг с прогрессом
                long remainingLines = Math.max(0, status.total - status.processed);
                double speed = Math.max(1, status.parsingSpeed);
                long parsingRemainingMs = (long) ((remainingLines / speed) * 1000);
                totalRemaining += Math.max(1000, Math.min(parsingRemainingMs, 60 * 60 * 1000));
            } else {
                // Нет данных о прогрессе - добавляем половину от базового времени
                totalRemaining += baseParsingTime / 2;
            }
        }

        // ЭТАП ФИНАЛИЗАЦИИ
        if (!status.finalizationCompleted) {
            if (stage.contains("Финализация") || stage.contains("🗃️ Финализация")) {
                // Текущий этап - считаем оставшееся время
                long stageElapsed = status.stageStartTime > 0 ?
                        Math.max(0, now - status.stageStartTime) : 0;

                int progress = (int) Math.min(99, Math.max(0, status.stageProgress));

                if (progress > 0 && stageElapsed > 0) {
                    // На основе текущей скорости
                    double progressPerMs = progress / (double) stageElapsed;
                    long remainingByActual = (long) ((100 - progress) / progressPerMs);
                    totalRemaining += Math.max(1000, Math.min(remainingByActual, 15 * 60 * 1000));
                } else {
                    // Нет данных о прогрессе
                    totalRemaining += estimatedFinalizationTime;
                }
            } else {
                // Этап еще не начат
                totalRemaining += estimatedFinalizationTime;
            }
        }

        // ЭТАП ИНДЕКСАЦИИ
        if (!status.indexingCompleted) {
            if (stage.contains("Индексация") || stage.contains("📈 Создание индексов")) {
                long stageElapsed = status.stageStartTime > 0 ?
                        Math.max(0, now - status.stageStartTime) : 0;

                int progress = (int) Math.min(99, Math.max(0, status.stageProgress));

                if (progress > 0 && stageElapsed > 0) {
                    double progressPerMs = progress / (double) stageElapsed;
                    long remainingByActual = (long) ((100 - progress) / progressPerMs);
                    totalRemaining += Math.max(1000, Math.min(remainingByActual, 30 * 60 * 1000));
                } else {
                    totalRemaining += estimatedIndexingTime;
                }
            } else {
                totalRemaining += estimatedIndexingTime;
            }
        }

        // ЭТАП СТАТИСТИКИ
        if (!status.statisticsCompleted) {
            if (stage.contains("Статистика") || stage.contains("📊 Обновление статистики")) {
                long stageElapsed = status.stageStartTime > 0 ?
                        Math.max(0, now - status.stageStartTime) : 0;

                int progress = (int) Math.min(99, Math.max(0, status.stageProgress));

                if (progress > 0 && stageElapsed > 0) {
                    double progressPerMs = progress / (double) stageElapsed;
                    long remainingByActual = (long) ((100 - progress) / progressPerMs);
                    totalRemaining += Math.max(1000, Math.min(remainingByActual, 15 * 60 * 1000));
                } else {
                    totalRemaining += estimatedStatisticsTime;
                }
            } else {
                totalRemaining += estimatedStatisticsTime;
            }
        }

        // ФИНАЛЬНЫЕ ОГРАНИЧЕНИЯ
        totalRemaining = Math.max(1000, Math.min(totalRemaining, 2 * 60 * 60 * 1000)); // от 1 сек до 2 часов

        return totalRemaining;
    }

    /**
     * Расчет оставшегося времени текущего этапа
     */
    private Map<String, Object> calculateCurrentStageRemaining(ParsingStatus status) {
        Map<String, Object> result = new HashMap<>();
        long remainingSeconds = -1;
        String remainingText = "расчет...";

        if (status == null) {
            result.put("remainingSeconds", -1);
            result.put("remaining", "нет данных");
            return result;
        }

        String stage = status.stageName != null ? status.stageName : "";
        long now = System.currentTimeMillis();

        try {
            // ===== СПЕЦИАЛЬНАЯ ОБРАБОТКА ДЛЯ ПОДСЧЕТА СТРОК =====
            if (stage.contains("Подсчет строк") || stage.contains("📊 Подсчет")) {
                // Для подсчета строк - быстрая фиксированная оценка
                remainingSeconds = 5; // максимум 5 секунд на подсчет
                remainingText = "~5 сек";
            }
            // ===== ЭТАП ПАРСИНГА =====
            else if (stage.contains("Парсинг") || stage.contains("🚀 Парсинг")) {
                if (status.parsingSpeed > 0.001 && status.total > 0 && status.processed > 0) {
                    long remainingLines = Math.max(0, status.total - status.processed);
                    double speed = Math.max(1, status.parsingSpeed); // минимум 1 стр/сек
                    remainingSeconds = (long) (remainingLines / speed);

                    // Ограничиваем разумными пределами
                    remainingSeconds = Math.max(1, Math.min(remainingSeconds, 3600)); // макс 1 час
                    remainingText = formatRemainingTime(remainingSeconds * 1000);
                } else {
                    // Нет данных о скорости - даем примерную оценку
                    if (status.total > 0) {
                        remainingSeconds = Math.min(300, status.total / 1000); // ~1000 строк/сек
                        remainingSeconds = Math.max(10, remainingSeconds);
                        remainingText = "~" + remainingSeconds + " сек";
                    }
                }
            }
            // ===== ЭТАП ФИНАЛИЗАЦИИ =====
            else if (stage.contains("Финализация") || stage.contains("🗃️ Финализация")) {
                long stageElapsed = status.stageStartTime > 0 ?
                        Math.max(0, now - status.stageStartTime) : 0;
                int progress = (int) Math.min(99, Math.max(0, status.stageProgress));

                if (progress > 0 && stageElapsed > 0) {
                    double progressPerMs = progress / (double) stageElapsed;
                    remainingSeconds = (long) ((100 - progress) / progressPerMs / 1000);
                } else if (status.estimatedFinalizationTime > 0) {
                    remainingSeconds = status.estimatedFinalizationTime / 1000;
                } else {
                    remainingSeconds = 30; // 30 секунд по умолчанию
                }

                remainingSeconds = Math.max(1, Math.min(remainingSeconds, 600)); // макс 10 минут
                remainingText = formatRemainingTime(remainingSeconds * 1000);
            }
            // ===== ЭТАП ИНДЕКСАЦИИ =====
            else if (stage.contains("Индексация") || stage.contains("📈 Создание индексов")) {
                long stageElapsed = status.stageStartTime > 0 ?
                        Math.max(0, now - status.stageStartTime) : 0;
                int progress = (int) Math.min(99, Math.max(0, status.stageProgress));

                if (progress > 0 && stageElapsed > 0) {
                    double progressPerMs = progress / (double) stageElapsed;
                    remainingSeconds = (long) ((100 - progress) / progressPerMs / 1000);
                } else if (status.estimatedIndexingTime > 0) {
                    remainingSeconds = status.estimatedIndexingTime / 1000;
                } else {
                    remainingSeconds = 60; // 1 минута по умолчанию
                }

                remainingSeconds = Math.max(1, Math.min(remainingSeconds, 1800)); // макс 30 минут
                remainingText = formatRemainingTime(remainingSeconds * 1000);
            }
            // ===== ЭТАП СТАТИСТИКИ =====
            else if (stage.contains("Статистика") || stage.contains("📊 Обновление статистики")) {
                long stageElapsed = status.stageStartTime > 0 ?
                        Math.max(0, now - status.stageStartTime) : 0;
                int progress = (int) Math.min(99, Math.max(0, status.stageProgress));

                if (progress > 0 && stageElapsed > 0) {
                    double progressPerMs = progress / (double) stageElapsed;
                    remainingSeconds = (long) ((100 - progress) / progressPerMs / 1000);
                } else if (status.estimatedStatisticsTime > 0) {
                    remainingSeconds = status.estimatedStatisticsTime / 1000;
                } else {
                    remainingSeconds = 45; // 45 секунд по умолчанию
                }

                remainingSeconds = Math.max(1, Math.min(remainingSeconds, 900)); // макс 15 минут
                remainingText = formatRemainingTime(remainingSeconds * 1000);
            }
        } catch (Exception e) {
            // При любой ошибке возвращаем безопасные значения
            remainingSeconds = 30;
            remainingText = "~30 сек";
        }

        result.put("remainingSeconds", remainingSeconds);
        result.put("remaining", remainingText);
        return result;
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
    private long calculateRemainingTimeForStage(long stageElapsed,
                                                int stageProgress,
                                                long estimatedTime) {
        if (stageProgress <= 0) return estimatedTime;
        if (stageProgress >= 100) return 0;

        long remainingByEstimate = (long)(estimatedTime * (100 - stageProgress) / 100.0);

        if (stageElapsed > 5000 && stageProgress > 5) {
            double progressPerMs = stageProgress / (double)stageElapsed;
            long remainingByActual = (long)((100 - stageProgress) / progressPerMs);

            double actualWeight = Math.min(0.9, stageProgress / 100.0);
            return (long)(remainingByActual * actualWeight +
                    remainingByEstimate * (1 - actualWeight));
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
            System.out.println("Отмена: парсинг не выполняется");
            return false;
        }

        System.out.println("🚫 Запрос на отмену парсинга...");
        currentStatus.isCancelled = true;

        // 1. Прерываем основной поток
        if (parsingTask != null && !parsingTask.isDone()) {
            parsingTask.cancel(true);
        }

        // 2. Вызываем cleanup в LogFileParser
        try {
            logFileParser.cleanup();
        } catch (Exception e) {
            System.err.println("Ошибка при очистке ресурсов: " + e.getMessage());
        }

        // 3. Прерываем активные соединения с БД
        new Thread(() -> {
            try (Connection cancelConn = DriverManager.getConnection(
                    DatabaseConfig.DB_URL,
                    DatabaseConfig.DB_USERNAME,
                    DatabaseConfig.DB_PASSWORD)) {

                // Находим и прерываем наш бэкенд процесс
                String findPidSql = "SELECT pid FROM pg_stat_activity " +
                        "WHERE usename = ? AND state = 'active' " +
                        "AND query LIKE '%COPY%' OR query LIKE '%CREATE INDEX%' " +
                        "ORDER BY backend_start DESC LIMIT 1";

                try (PreparedStatement ps = cancelConn.prepareStatement(findPidSql)) {
                    ps.setString(1, DatabaseConfig.DB_USERNAME);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            int pid = rs.getInt("pid");
                            try (Statement stmt = cancelConn.createStatement()) {
                                stmt.execute("SELECT pg_terminate_backend(" + pid + ")");
                                System.out.println("✅ Бэкенд процесс " + pid + " завершен");
                            }
                        }
                    }
                }

                // Отменяем текущую транзакцию
                try (Statement stmt = cancelConn.createStatement()) {
                    stmt.execute("SELECT pg_cancel_backend(pg_backend_pid())");
                }

            } catch (SQLException e) {
                System.out.println("ℹ️ Отмена БД: " + e.getMessage());
            }
        }).start();

        // 4. Обновляем статус
        currentStatus.isParsing = false;
        currentStatus.isCancelled = true;
        currentStatus.status = "🚫 Парсинг отменен пользователем";
        currentStatus.stageName = "Отменено";
        currentStatus.progress = 0;
        currentStatus.stageProgress = 0;
        currentStatus.estimatedTimeRemaining = 0;
        currentStatus.parsingCompleted = false;
        currentStatus.finalizationCompleted = false;
        currentStatus.indexingCompleted = false;
        currentStatus.statisticsCompleted = false;
        currentStatus.parsingSpeed = 0;
        currentStatus.processed = 0;
        currentStatus.total = 0;

        System.out.println("✅ Запрос на отмену отправлен");
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