package com.work.LogParser.model;

public class ParsingStatus {
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

    // НОВЫЕ ПОЛЯ ДЛЯ РАСЧЕТА ВРЕМЕНИ
    public long parsingDuration = 0;
    public long estimatedFinalizationTime = 0;
    public long estimatedIndexingTime = 0;
    public long estimatedStatisticsTime = 0;

    // ДОБАВИТЬ ЭТИ ПОЛЯ:
    public double parsingSpeed = 0;           // Текущая скорость обработки (строк/сек)
    public long parsingStageStartTime = 0;    // Время начала этапа парсинга
    public long lastProgressUpdateTime = 0;   // Время последнего обновления прогресса
    public long lastProcessedCount = 0;       // Последнее количество обработанных строк
}