package com.work.LogParser.controller;

import com.work.LogParser.service.AggregatedStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/stats")
@CrossOrigin(origins = "*")
public class StatsController {

    @Autowired
    private AggregatedStatsService aggregatedStatsService;

    /**
     * Принудительно пересчитать дефолтную статистику
     */
    @PostMapping("/recalculate-default")
    public ResponseEntity<?> recalculateDefaultStats() {
        try {
            aggregatedStatsService.calculateAndSaveDefaultStats();
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Дефолтная статистика пересчитана"
            ));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of(
                    "success", false,
                    "error", "Ошибка пересчета статистики: " + e.getMessage()
            ));
        }
    }

    /**
     * Получить информацию об агрегированной статистике
     */
    @GetMapping("/info")
    public ResponseEntity<?> getStatsInfo() {
        try {
            Map<String, Object> info = new HashMap<>();

            // Можно добавить дополнительную информацию о статистике

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "info", info,
                    "message", "Используется агрегированная статистика для ускорения загрузки"
            ));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of(
                    "success", false,
                    "error", "Ошибка получения информации: " + e.getMessage()
            ));
        }
    }
}