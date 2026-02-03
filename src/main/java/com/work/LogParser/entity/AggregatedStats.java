package com.work.LogParser.model;

import java.time.LocalDateTime;

public class AggregatedStats {
    private Long id;
    private LocalDateTime periodStart;
    private LocalDateTime periodEnd;
    private long totalRequests;
    private long errorRequests;
    private long uniqueIps;
    private double avgResponseTime;
    private double totalTrafficMb;
    private String statusDistributionJson;
    private String hourlyDistributionJson;
    private LocalDateTime createdAt;
    private boolean isDefault; // Флаг для статистики по всем данным

    // Конструкторы
    public AggregatedStats() {
        this.createdAt = LocalDateTime.now();
        this.isDefault = false;
    }

    public AggregatedStats(LocalDateTime periodStart, LocalDateTime periodEnd,
                           long totalRequests, long errorRequests, long uniqueIps,
                           double avgResponseTime, double totalTrafficMb,
                           String statusDistributionJson, String hourlyDistributionJson) {
        this();
        this.periodStart = periodStart;
        this.periodEnd = periodEnd;
        this.totalRequests = totalRequests;
        this.errorRequests = errorRequests;
        this.uniqueIps = uniqueIps;
        this.avgResponseTime = avgResponseTime;
        this.totalTrafficMb = totalTrafficMb;
        this.statusDistributionJson = statusDistributionJson;
        this.hourlyDistributionJson = hourlyDistributionJson;
    }

    // Геттеры и сеттеры
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public LocalDateTime getPeriodStart() { return periodStart; }
    public void setPeriodStart(LocalDateTime periodStart) { this.periodStart = periodStart; }

    public LocalDateTime getPeriodEnd() { return periodEnd; }
    public void setPeriodEnd(LocalDateTime periodEnd) { this.periodEnd = periodEnd; }

    public long getTotalRequests() { return totalRequests; }
    public void setTotalRequests(long totalRequests) { this.totalRequests = totalRequests; }

    public long getErrorRequests() { return errorRequests; }
    public void setErrorRequests(long errorRequests) { this.errorRequests = errorRequests; }

    public long getUniqueIps() { return uniqueIps; }
    public void setUniqueIps(long uniqueIps) { this.uniqueIps = uniqueIps; }

    public double getAvgResponseTime() { return avgResponseTime; }
    public void setAvgResponseTime(double avgResponseTime) { this.avgResponseTime = avgResponseTime; }

    public double getTotalTrafficMb() { return totalTrafficMb; }
    public void setTotalTrafficMb(double totalTrafficMb) { this.totalTrafficMb = totalTrafficMb; }

    public String getStatusDistributionJson() { return statusDistributionJson; }
    public void setStatusDistributionJson(String statusDistributionJson) { this.statusDistributionJson = statusDistributionJson; }

    public String getHourlyDistributionJson() { return hourlyDistributionJson; }
    public void setHourlyDistributionJson(String hourlyDistributionJson) { this.hourlyDistributionJson = hourlyDistributionJson; }

    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

    public boolean isDefault() { return isDefault; }
    public void setDefault(boolean isDefault) { this.isDefault = isDefault; }
}