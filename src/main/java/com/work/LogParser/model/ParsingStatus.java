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

    // ФАКТИЧЕСКОЕ ВРЕМЯ ЭТАПОВ
    public long parsingDuration = 0;
    public long finalizationDuration = 0;
    public long indexingDuration = 0;
    public long statisticsDuration = 0;

    // ОЦЕНОЧНОЕ ВРЕМЯ
    public long estimatedParsingTime = 0;      // Время этапа парсинга
    public long estimatedFinalizationTime = 0;
    public long estimatedIndexingTime = 0;
    public long estimatedStatisticsTime = 0;

    // ДЛЯ АДАПТИВНЫХ КОЭФФИЦИЕНТОВ
    public double finalizationFactor = 1.16;
    public double indexingFactor = 0.03;
    public double statisticsFactor = 1.10;

    public long estimatedTotalTime = 0;


}