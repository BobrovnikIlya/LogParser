package com.work.LogParser.config;

import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.concurrent.TimeUnit;

@Configuration
@EnableCaching
public class CacheConfig {

    @Bean
    @Primary
    public CacheManager cacheManager() {
        com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeineCache = Caffeine.newBuilder()
                .initialCapacity(100)
                .maximumSize(1000)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .recordStats()
                .build();

        // Используем простую реализацию для начала
        org.springframework.cache.concurrent.ConcurrentMapCacheManager cacheManager =
                new org.springframework.cache.concurrent.ConcurrentMapCacheManager(
                        "filteredResults",    // Кэш для запросов с фильтрами
                        "topUrls",           // Кэш для топ URL
                        "topUsers"          // Кэш для топ пользователей
                );

        return cacheManager;
    }


    // Либо если нужны два менеджера, добавьте:
    @Bean(name = "longTermCacheManager")
    public CacheManager longTermCacheManager() {
        com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeineCache = Caffeine.newBuilder()
                .initialCapacity(50)
                .maximumSize(200)
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .build();

        org.springframework.cache.concurrent.ConcurrentMapCacheManager cacheManager =
                new org.springframework.cache.concurrent.ConcurrentMapCacheManager("defaultFilters");

        return cacheManager;
    }
}