package com.work.LogParser.service;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Service
public class FilterCacheService {

    @Cacheable(value = "filterCache", key = "#cacheKey")
    public Map<String, Object> getCachedFilterResults(String cacheKey,
                                                      Supplier<Map<String, Object>> dataLoader) {
        return dataLoader.get();
    }

    // Новые методы с проверкой на пустые фильтры
    @Cacheable(value = "topUrlsCache", key = "#cacheKey")
    public List<Map<String, Object>> getCachedTopUrls(String cacheKey,
                                                      Supplier<List<Map<String, Object>>> dataLoader,
                                                      boolean areFiltersEmpty) {
        return dataLoader.get();
    }

    @Cacheable(value = "topUsersCache", key = "#cacheKey")
    public List<Map<String, Object>> getCachedTopUsers(String cacheKey,
                                                       Supplier<List<Map<String, Object>>> dataLoader,
                                                       boolean areFiltersEmpty) {
        return dataLoader.get();
    }

    // Генерация ключа кэша на основе фильтров
    public String generateCacheKey(String dateFrom, String dateTo, String ip,
                                   String username, String status, String action) {
        return String.format("%s|%s|%s|%s|%s|%s",
                dateFrom, dateTo, ip, username, status, action);
    }

    // Генерация ключа для топов с учетом фильтров
    public String generateTopCacheKey(String dateFrom, String dateTo, String ip,
                                      String username, String status, String action, String type, int limit) {
        return String.format("%s|%s|%s|%s|%s|%s|%s|%d",
                dateFrom, dateTo, ip, username, status, action, type, limit);
    }

    // Проверка, пустые ли фильтры (дефолтный случай)
    public boolean areFiltersEmpty(String dateFrom, String dateTo, String ip,
                                   String username, String status, String action) {
        return (dateFrom == null || dateFrom.isEmpty()) &&
                (dateTo == null || dateTo.isEmpty()) &&
                (ip == null || ip.isEmpty()) &&
                (username == null || username.isEmpty()) &&
                (status == null || status.isEmpty()) &&
                (action == null || action.isEmpty());
    }

    // Ключ для дефолтных (пустых) фильтров
    public String getDefaultTopCacheKey(String type, int limit) {
        return String.format("DEFAULT|%s|%d", type, limit);
    }
}