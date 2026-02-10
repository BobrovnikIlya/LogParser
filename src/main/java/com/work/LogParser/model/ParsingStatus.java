package com.work.LogParser.model;

public class ParsingStatus {
    public boolean isParsing = false;
    public boolean isCancelled = false;
    public String status = "";
    public double progress = 0;          // Общий прогресс 0-100
    public double stageProgress = 0;     // Прогресс текущего этапа 0-100
    public String stageName = "";        // Название текущего этапа
    public long processed = 0;
    public long total = 0;
    public String filePath = "";
    public long startTime = 0;
}