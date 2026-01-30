package com.work.LogParser.model;

public class ParsingStatus {
    public boolean isParsing = false;
    public String status = "Готов к работе";
    public double progress = 0;
    public long processed = 0;
    public long total = 0;
    public String filePath;
    public long startTime;
    public volatile boolean isCancelled = false;
}