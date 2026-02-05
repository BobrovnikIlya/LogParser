package com.work.LogParser.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Caching;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

@Service
public class FilterCacheService {

    private final CacheManager cacheManager;

    // Локальный кэш для быстрого доступа к пустым фильтрам
    private final ConcurrentHashMap<String, CacheEntry<?>> memoryCache = new ConcurrentHashMap<>();

    // Класс для записи в кэш с временем создания
    private static class CacheEntry<T> {
        private final T data;
        private final long timestamp;

        public CacheEntry(T data) {
            this.data = data;
            this.timestamp = System.currentTimeMillis();
        }

        public T getData() {
            return data;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public boolean isExpired(long ttlMillis) {
            return System.currentTimeMillis() - timestamp > ttlMillis;
        }
    }

    // Время жизни кэша в миллисекундах
    private static final long CACHE_TTL_MS = 5 * 60 * 1000; // 5 минут
    private static final long DEFAULT_FILTERS_TTL_MS = 30 * 60 * 1000; // 30 минут для дефолтных

    @Autowired
    public FilterCacheService(@Qualifier("cacheManager") CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    /**
     * Получение данных с кэшированием с разделением по типам фильтров
     */
    public Map<String, Object> getCachedFilterResults(String cacheKey,
                                                      Supplier<Map<String, Object>> dataLoader,
                                                      String... filters) {

        boolean isEmptyFilter = areFiltersEmpty(cacheKey);

        // Для пустых фильтров используем отдельную стратегию с более длительным TTL
        if (isEmptyFilter) {
            return getFromCacheOrLoad("defaultFilters", cacheKey, dataLoader, DEFAULT_FILTERS_TTL_MS);
        }

        // Для фильтрованных запросов используем обычный кэш с проверкой TTL
        return getFromCacheOrLoad("filteredResults", cacheKey, dataLoader, CACHE_TTL_MS);
    }

    /**
     * Получение топ URL с кэшированием
     */
    public List<Map<String, Object>> getCachedTopUrls(String cacheKey,
                                                      Supplier<List<Map<String, Object>>> dataLoader,
                                                      boolean areFiltersEmpty) {

        if (areFiltersEmpty) {
            // Для пустых фильтров используем прерассчитанные данные (не кэшируем здесь)
            return dataLoader.get();
        }

        // Для фильтрованных запросов кэшируем
        return getFromCacheOrLoad("topUrls", cacheKey, dataLoader, CACHE_TTL_MS);
    }

    /**
     * Получение топ пользователей с кэшированием
     */
    public List<Map<String, Object>> getCachedTopUsers(String cacheKey,
                                                       Supplier<List<Map<String, Object>>> dataLoader,
                                                       boolean areFiltersEmpty) {

        if (areFiltersEmpty) {
            // Для пустых фильтров используем прерассчитанные данные
            return dataLoader.get();
        }

        // Для фильтрованных запросов кэшируем
        return getFromCacheOrLoad("topUsers", cacheKey, dataLoader, CACHE_TTL_MS);
    }

    /**
     * Универсальный метод получения из кэша или загрузки
     */
    @SuppressWarnings("unchecked")
    private <T> T getFromCacheOrLoad(String cacheName, String cacheKey,
                                     Supplier<T> dataLoader, long ttlMs) {

        // Проверяем сначала в memoryCache
        CacheEntry<T> cached = (CacheEntry<T>) memoryCache.get(cacheKey);

        if (cached != null) {
            if (!cached.isExpired(ttlMs)) {
                System.out.println("✅ Данные из памяти для ключа: " + cacheKey);
                return cached.getData();
            } else {
                // Удаляем просроченные данные
                memoryCache.remove(cacheKey);
            }
        }

        // Проверяем в Spring Cache (если настроен)
        Cache springCache = cacheManager.getCache(cacheName);
        if (springCache != null) {
            Cache.ValueWrapper cachedValue = springCache.get(cacheKey);
            if (cachedValue != null) {
                System.out.println("✅ Данные из Spring Cache для ключа: " + cacheKey);
                T data = (T) cachedValue.get();
                // Сохраняем также в memory cache для быстрого доступа
                memoryCache.put(cacheKey, new CacheEntry<>(data));
                return data;
            }
        }

        // Если нет в кэше - загружаем
        System.out.println("⏳ Загрузка данных для ключа: " + cacheKey);
        T data = dataLoader.get();

        // Сохраняем в memory cache
        memoryCache.put(cacheKey, new CacheEntry<>(data));

        // Сохраняем в Spring Cache
        if (springCache != null) {
            springCache.put(cacheKey, data);
        }

        return data;
    }

    /**
     * Генерация ключа кэша
     */
    public String generateCacheKey(String dateFrom, String dateTo, String ip,
                                   String username, String status, String action) {
        // Нормализуем значения для ключа
        String normalizedDateFrom = normalizeDate(dateFrom);
        String normalizedDateTo = normalizeDate(dateTo);
        String normalizedIp = ip != null ? ip.trim().toLowerCase() : "";
        String normalizedUsername = username != null ? username.trim().toLowerCase() : "";
        String normalizedStatus = status != null ? status.trim() : "";
        String normalizedAction = action != null ? action.trim().toLowerCase() : "";

        return String.format("filter:%s:%s:%s:%s:%s:%s",
                normalizedDateFrom, normalizedDateTo,
                normalizedIp, normalizedUsername,
                normalizedStatus, normalizedAction);
    }

    /**
     * Генерация ключа для топов
     */
    public String generateTopCacheKey(String dateFrom, String dateTo, String ip,
                                      String username, String status, String action,
                                      String type, int limit) {
        String filterKey = generateCacheKey(dateFrom, dateTo, ip, username, status, action);
        return String.format("top:%s:%s:%d", type, filterKey, limit);
    }

    /**
     * Проверка на пустые фильтры
     */
    public boolean areFiltersEmpty(String dateFrom, String dateTo, String ip,
                                   String username, String status, String action) {
        return (dateFrom == null || dateFrom.trim().isEmpty()) &&
                (dateTo == null || dateTo.trim().isEmpty()) &&
                (ip == null || ip.trim().isEmpty()) &&
                (username == null || username.trim().isEmpty()) &&
                (status == null || status.trim().isEmpty()) &&
                (action == null || action.trim().isEmpty());
    }

    /**
     * Проверка на пустые фильтры по ключу кэша
     */
    private boolean areFiltersEmpty(String cacheKey) {
        // Если ключ содержит только пустые значения после "filter:"
        if (cacheKey.startsWith("filter:")) {
            String[] parts = cacheKey.split(":");
            if (parts.length >= 8) {
                for (int i = 1; i < 7; i++) {
                    if (!parts[i].isEmpty()) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Нормализация даты для ключа
     */
    private String normalizeDate(String date) {
        if (date == null || date.trim().isEmpty()) {
            return "";
        }
        // Убираем время, если есть, оставляем только дату для группировки
        String trimmed = date.trim();
        if (trimmed.contains(" ")) {
            return trimmed.substring(0, trimmed.indexOf(" "));
        }
        if (trimmed.contains("T")) {
            return trimmed.substring(0, trimmed.indexOf("T"));
        }
        return trimmed;
    }

    /**
     * Очистка кэша по ключу
     */
    @Caching(evict = {
            @CacheEvict(value = "filteredResults", key = "#cacheKey"),
            @CacheEvict(value = "topUrls", key = "#cacheKey + ':top:urls:*'"),
            @CacheEvict(value = "topUsers", key = "#cacheKey + ':top:users:*'")
    })
    public void evictCache(String cacheKey) {
        memoryCache.remove(cacheKey);
        System.out.println("🗑️ Кэш очищен для ключа: " + cacheKey);
    }

    /**
     * Очистка всего кэша
     */
    @Caching(evict = {
            @CacheEvict(value = "filteredResults", allEntries = true),
            @CacheEvict(value = "topUrls", allEntries = true),
            @CacheEvict(value = "topUsers", allEntries = true)
    })
    public void evictAllCache() {
        memoryCache.clear();
        System.out.println("🗑️ Весь кэш очищен");
    }

    /**
     * Периодическая очистка просроченного кэша
     */
    @Scheduled(fixedDelay = 60000) // Каждую минуту
    public void cleanupExpiredCache() {
        int removed = 0;
        long now = System.currentTimeMillis();

        for (var entry : memoryCache.entrySet()) {
            CacheEntry<?> cacheEntry = entry.getValue();
            String key = entry.getKey();

            // Определяем TTL в зависимости от типа ключа
            long ttl = key.startsWith("filter:") ? CACHE_TTL_MS : DEFAULT_FILTERS_TTL_MS;

            if (cacheEntry.isExpired(ttl)) {
                memoryCache.remove(key);
                removed++;
            }
        }

        if (removed > 0) {
            System.out.println("🧹 Очищено просроченных записей: " + removed);
        }
    }

    /**
     * Метод для принудительного обновления кэша при изменении данных
     */
    public void invalidateCacheAfterDataChange() {
        // Очищаем только кэш фильтрованных данных, дефолтные данные остаются
        for (var entry : memoryCache.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith("filter:::")) { // Не очищаем дефолтные
                memoryCache.remove(key);
            }
        }

        // Очищаем Spring Cache
        Cache filteredCache = cacheManager.getCache("filteredResults");
        if (filteredCache != null) {
            filteredCache.clear();
        }

        System.out.println("🔄 Кэш фильтров обновлен после изменения данных");
    }
}