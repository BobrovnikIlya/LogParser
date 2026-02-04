package com.work.LogParser.service;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.function.Supplier;

@Service
public class FilterCacheService {

    @Cacheable(value = "filterCache", key = "#cacheKey")
    public Map<String, Object> getCachedFilterResults(String cacheKey,
                                                      Supplier<Map<String, Object>> dataLoader) {
        return dataLoader.get();
    }

    // Генерация ключа кэша на основе фильтров
    public String generateCacheKey(String dateFrom, String dateTo, String ip,
                                   String username, String status, String action) {
        return String.format("%s|%s|%s|%s|%s|%s",
                dateFrom, dateTo, ip, username, status, action);
    }

    // Иерархический кэш:
    // 1. Redis/Memcached - долгоживущие результаты (30 мин)
    // 2. Caffeine/EHCache - быстрые in-memory (5 мин)
    // 3. PostgreSQL materialized views - предвычисленные агрегаты
}