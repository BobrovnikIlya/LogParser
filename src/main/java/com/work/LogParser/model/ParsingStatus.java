package com.work.LogParser.model;

public class ParsingStatus {
    // Существующие поля
    public boolean isParsing = false;
    public boolean isCancelled = false;
    public String status = "";
    public double progress = 0;
    public double stageProgress = 0;
    public String stageName = "";
    public long processed = 0;
    public long total = 0;
    public String filePath = "";
    public long startTime = 0;

    // Существующие поля для расчетов
    public long parsingDuration = 0;
    public long estimatedFinalizationTime = 0;
    public long estimatedIndexingTime = 0;
    public long estimatedStatisticsTime = 0;
    public double parsingSpeed = 0;
    public long parsingStageStartTime = 0;
    public long lastProgressUpdateTime = 0;
    public long lastProcessedCount = 0;

    public long estimatedTimeRemaining = 0;

    // Фактическое время выполнения этапов (мс)
    public long actualParsingTime = 0;
    public long actualFinalizationTime = 0;
    public long actualIndexingTime = 0;
    public long actualStatisticsTime = 0;

    // Флаги завершения этапов
    public boolean parsingCompleted = false;
    public boolean finalizationCompleted = false;
    public boolean indexingCompleted = false;
    public boolean statisticsCompleted = false;

    // Время начала текущего этапа
    public long stageStartTime = 0;

    // Для отслеживания прогресса индексации
    public int indexesCreated = 0;
    public int totalIndexes = 6; // Общее количество индексов

    // Для отслеживания прогресса статистики
    public String currentStatisticTask = "";
    public int statisticsSubProgress = 0;

    private volatile boolean cleanupRequested = false;
    private String cancellationReason = null;
    private long cancellationTime = 0;

    // Геттеры и сеттеры
    public boolean isCleanupRequested() {
        return cleanupRequested;
    }

    public void setCleanupRequested(boolean cleanupRequested) {
        this.cleanupRequested = cleanupRequested;
    }

    public String getCancellationReason() {
        return cancellationReason;
    }

    public void setCancellationReason(String cancellationReason) {
        this.cancellationReason = cancellationReason;
    }

    public long getCancellationTime() {
        return cancellationTime;
    }

    public void setCancellationTime(long cancellationTime) {
        this.cancellationTime = cancellationTime;
    }
}