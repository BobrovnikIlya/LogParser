package com.work.LogParser.service;

import com.work.LogParser.config.DatabaseConfig;
import com.work.LogParser.model.ParsingStatus;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

@Service
public class LogFileParser {

    @Autowired
    private DatabaseManager databaseManager;

    @Autowired
    private LogParserUtils logParserUtils;
    @Autowired
    private PrecalculatedTopService precalculatedTopService;

    @Autowired
    private AggregatedStatsService aggregatedStatsService;

    private static final int MEMORY_BUFFER_SIZE = 100 * 1024 * 1024; // 100 MB
    private static final int COPY_BUFFER_SIZE = 64 * 1024; // 64 KB для COPY
    private static final int BATCH_COMMIT_SIZE = 100000; // 100K записей на транзакцию

    private static final Pattern LOG_PATTERN = Pattern.compile(
            "^" +
                    "(\\d+\\.\\d+)\\s+" +              // 1. timestamp
                    "(\\d+)\\s+" +                     // 2. response_time_ms
                    "(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+" + // 3. client_ip
                    "([A-Z_]+)(?:/(\\d{3}))?\\s+" +    // 4. action и 5. status_code (опционально)
                    "(\\d+)\\s+" +                     // 6. response_size_bytes
                    "(\\S+)\\s+" +                     // 7. http_method
                    "(\\S+)\\s+" +                     // 8. url
                    "(\\S+|-)\\s+" +                   // 9. username
                    "(\\S+)\\s+" +                     // 10. hierarchy
                    "(\\S+)"                           // 11. content_type
    );

    private BufferedReader createOptimizedReader(String filePath) throws IOException {
        System.out.println("Создание оптимизированного reader для файла: " + filePath);

        FileChannel channel = FileChannel.open(
                Paths.get(filePath),
                StandardOpenOption.READ
        );

        // Используем прямой буфер для максимальной скорости
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocateDirect(2 * 1024 * 1024); // 2MB

        return new BufferedReader(
                new InputStreamReader(
                        Channels.newInputStream(channel),
                        StandardCharsets.UTF_8
                ),
                4 * 1024 * 1024 // 4MB буфер чтения
        );
    }

    public void parseWithHybridCopy(String filePath, ParsingStatus currentStatus) {
        long startTime = System.currentTimeMillis();
        long totalRecords = 0;
        long totalLines = 0;

        // Временные метки для каждого этапа
        long parsingStageStartTime = 0;
        long parsingStageDuration = 0;

        // Веса этапов
        final double COUNTING_WEIGHT = 0.0044;     // 0.44%
        final double PARSING_WEIGHT = 0.3026;      // 30.26%
        final double FINALIZATION_WEIGHT = 0.3509; // 35.09%
        final double INDEXING_WEIGHT = 0.0088;     // 0.88%
        final double STATISTICS_WEIGHT = 0.3333;   // 33.33%

        System.out.println("Начало гибридного парсинга с оптимизацией...");

        try (Connection conn = DriverManager.getConnection(DatabaseConfig.DB_URL, DatabaseConfig.DB_USERNAME, DatabaseConfig.DB_PASSWORD)) {

            // 1. Подготовка БД
            databaseManager.ensureLogsTableExists(conn);
            databaseManager.createStatusesTable(conn);
            databaseManager.createActionsTable(conn);

            if (!shouldParseLogs(conn, filePath)) {
                System.out.println("Парсинг не требуется");
                currentStatus.isParsing = false;
                currentStatus.progress = 100;
                currentStatus.stageProgress = 100;
                currentStatus.stageName = "Готово";
                return;
            }

            // 2. Подсчет строк (5% общего прогресса)
            currentStatus.stageName = "📊 Подсчет строк";
            currentStatus.stageProgress = 0;
            currentStatus.progress = 0;

            long startCountTime = System.currentTimeMillis();  // ← ДОБАВЬ
            totalLines = estimateLineCountWithNIO(filePath);
            long countTime = System.currentTimeMillis() - startCountTime;  // ← ДОБАВЬ
            currentStatus.total = totalLines;

            System.out.println("🔥🔥🔥 ПОДСЧЕТ СТРОК ЗАНЯЛ: " + countTime + " мс");
            System.out.println("🔥🔥🔥 totalLines = " + totalLines);
            System.out.println("🔥🔥🔥 СТАВИМ stageProgress = 100");

            currentStatus.stageProgress = 100;
            currentStatus.progress = (int)(COUNTING_WEIGHT * 100);

            currentStatus.stageName = "🚀 Парсинг данных";
            currentStatus.stageProgress = 0;
            parsingStageStartTime = System.currentTimeMillis();

            long estimatedParsingTime = (long)(totalLines * 0.005); // 0.5мс на строку
            long estimatedTotalTime = (long)(estimatedParsingTime / PARSING_WEIGHT);
            long estimatedFinalizationTime = (long)(estimatedTotalTime * FINALIZATION_WEIGHT);
            long estimatedIndexingTime = (long)(estimatedTotalTime * INDEXING_WEIGHT);
            long estimatedStatisticsTime = (long)(estimatedTotalTime * STATISTICS_WEIGHT);

            currentStatus.estimatedTotalTime = estimatedTotalTime;
            currentStatus.estimatedParsingTime = estimatedParsingTime;
            currentStatus.estimatedFinalizationTime = estimatedFinalizationTime;
            currentStatus.estimatedIndexingTime = estimatedIndexingTime;
            currentStatus.estimatedStatisticsTime = estimatedStatisticsTime;

            currentStatus.status = String.format("Общее время: ~%d мин | Парсинг: ~%d сек | Финал: ~%d сек | Индексы: ~%d сек | Статистика: ~%d сек",
                    estimatedTotalTime / 60000,
                    estimatedParsingTime / 1000,
                    estimatedFinalizationTime / 1000,
                    estimatedIndexingTime / 1000,
                    estimatedStatisticsTime / 1000);

            // 3. Очистка и создание таблицы
            databaseManager.clearLogsTable(conn);
            databaseManager.createUnloggedTable(conn);

            // 4. Оптимизация настроек БД перед COPY
            databaseManager.prepareConnectionForCopy(conn);

            // 5. Гибридная загрузка
            System.out.println("Начало гибридной загрузки с оптимизированным чтением...");
            // Начало этапа парсинга

            // Создаем Piped потоки для потокового COPY
            PipedOutputStream pos = new PipedOutputStream();
            PipedInputStream pis = new PipedInputStream(pos, MEMORY_BUFFER_SIZE);

            // Запускаем COPY в отдельном потоке
            Thread copyThread = new Thread(() -> {
                performStreamingCopyWithOptimization(conn, pis, currentStatus);
            });
            copyThread.start();

            // Основной поток: парсинг с оптимизированным чтением
            try (BufferedReader br = createOptimizedReader(filePath);
                 BufferedWriter writer = new BufferedWriter(
                         new OutputStreamWriter(pos, StandardCharsets.UTF_8),
                         COPY_BUFFER_SIZE)) {

                String line;
                long lineNumber = 0;
                long recordsInBatch = 0;
                long batchStartTime = System.currentTimeMillis();

                while ((line = br.readLine()) != null) {
                    lineNumber++;

                    // Проверка отмены
                    if (currentStatus.isCancelled) {
                        System.out.println("Парсинг прерван");
                        break;
                    }

                    // Быстрый парсинг
                    String csvLine = parseLineToCSV(line);
                    if (csvLine != null) {
                        writer.write(csvLine);
                        writer.write('\n');
                        totalRecords++;
                        recordsInBatch++;

                        // Обновление прогресса каждые 5000 строк
                        if (lineNumber % 5000 == 0) {
                            currentStatus.processed = lineNumber;

                            double stageProgress = (lineNumber * 100.0) / totalLines;
                            double overallProgress = COUNTING_WEIGHT * 100 + (PARSING_WEIGHT * 100 * stageProgress / 100.0);

                            currentStatus.stageProgress = (int)stageProgress;
                            currentStatus.progress = (int)overallProgress;

                            long elapsedParsing = System.currentTimeMillis() - parsingStageStartTime;

                            if (stageProgress > 1.0) {
                                elapsedParsing = System.currentTimeMillis() - parsingStageStartTime;
                                double elapsedSec = elapsedParsing / 1000.0;

                                // СКОРОСТЬ (строк/сек)
                                double speed = lineNumber / elapsedSec;

                                // ОСТАЛОСЬ СТРОК
                                long remainingLines = totalLines - lineNumber;

                                // ОСТАЛОСЬ СЕКУНД
                                long remainingSec = (long)(remainingLines / speed);

                                // НОВОЕ ОБЩЕЕ ВРЕМЯ ПАРСИНГА = уже прошло + осталось
                                long newEstimatedParsingTime = elapsedParsing + (remainingSec * 1000);

                                // СОХРАНЯЕМ
                                currentStatus.estimatedParsingTime = newEstimatedParsingTime;
                                currentStatus.estimatedTotalTime = (long)(newEstimatedParsingTime / PARSING_WEIGHT);
                                currentStatus.estimatedFinalizationTime = (long)(currentStatus.estimatedTotalTime * FINALIZATION_WEIGHT);
                                currentStatus.estimatedIndexingTime = (long)(currentStatus.estimatedTotalTime * INDEXING_WEIGHT);
                                currentStatus.estimatedStatisticsTime = (long)(currentStatus.estimatedTotalTime * STATISTICS_WEIGHT);
                            }
                        }
                    }
                }

                // Финализация записи
                writer.flush();
                writer.close();

                // Замеряем время парсинга
                parsingStageDuration = System.currentTimeMillis() - parsingStageStartTime;
                System.out.println("Парсинг завершен за " + (parsingStageDuration / 1000.0) + " сек");

                // Устанавливаем 100% прогресс парсинга
                currentStatus.stageProgress = 100;
                currentStatus.progress = (int)(COUNTING_WEIGHT * 100 + PARSING_WEIGHT * 100);

            } catch (Exception e) {
                System.err.println("Ошибка при чтении/записи: " + e.getMessage());
                throw e;
            }

            // Ждем завершения COPY
            copyThread.join();

            // 6. Восстановление настроек БД
            databaseManager.restoreConnectionSettings(conn);
            estimatedFinalizationTime = currentStatus.estimatedFinalizationTime;
            // 7. completeProcessing
            if (totalRecords > 0 && !currentStatus.isCancelled) {

                completeProcessing(conn, currentStatus, startTime, totalLines, totalRecords,
                        COUNTING_WEIGHT, PARSING_WEIGHT, FINALIZATION_WEIGHT,
                        INDEXING_WEIGHT, STATISTICS_WEIGHT, parsingStageDuration);
            } else {
                finishWithNoData(currentStatus);
            }

        } catch (Exception e) {
            handleParsingError(currentStatus, e);
        }
    }

    private long estimateLineCountWithNIO(String filePath) throws IOException {
        File file = new File(filePath);
        long fileSize = file.length();

        // Порог для быстрой оценки
        if (fileSize > 50_000_000) { // > 50MB
            int SAMPLE_COUNT = 10;
            int SAMPLE_SIZE = 65536; // 64KB каждый

            try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
                byte[][] samples = new byte[SAMPLE_COUNT][SAMPLE_SIZE];
                long[] positions = new long[SAMPLE_COUNT];

                // Рассчитываем позиции для равномерного распределения
                for (int i = 0; i < SAMPLE_COUNT; i++) {
                    positions[i] = (fileSize * i) / SAMPLE_COUNT;
                }

                // Читаем образцы
                for (int i = 0; i < SAMPLE_COUNT; i++) {
                    long pos = positions[i];
                    // Корректируем позицию чтобы не выйти за границы
                    if (pos + SAMPLE_SIZE > fileSize) {
                        pos = fileSize - SAMPLE_SIZE;
                    }
                    if (pos < 0) pos = 0;

                    raf.seek(pos);
                    raf.read(samples[i]);
                }

                // Считаем строки во всех образцах
                double totalLines = 0;
                for (byte[] sample : samples) {
                    totalLines += countLinesInBuffer(sample);
                }

                // Среднее значение строк на 64KB
                double avgLinesPerSample = totalLines / SAMPLE_COUNT;

                // Оценка общего количества строк
                long estimatedLines = (long) ((fileSize / (double)SAMPLE_SIZE) * avgLinesPerSample);

                System.out.printf("Точная оценка (10 образцов): %,d строк (файл: %,d bytes, %.1f строк/64KB)%n",
                        estimatedLines, fileSize, avgLinesPerSample);

                return estimatedLines;
            }
        }

        // Для маленьких файлов считаем точно
        return countLinesAccurately(filePath);
    }

    private long countLinesInBuffer(byte[] buffer) {
        long lines = 0;
        for (int i = 0; i < buffer.length; i++) {
            if (buffer[i] == '\n') {
                lines++;
                // Учитываем \r\n для Windows
                if (i > 0 && buffer[i-1] == '\r') {
                    // Уже учли как \n, ничего не делаем
                }
            }
        }
        return lines;
    }

    private long countLinesAccurately(String filePath) throws IOException {
        try (LineNumberReader lnr = new LineNumberReader(
                new InputStreamReader(
                        new java.io.FileInputStream(filePath),
                        StandardCharsets.UTF_8
                )
        )) {
            lnr.skip(Long.MAX_VALUE);
            return lnr.getLineNumber() + 1;
        }
    }

    private void performStreamingCopyWithOptimization(Connection conn, InputStream dataStream, ParsingStatus status) {
        System.out.println("Запуск оптимизированного потокового COPY...");

        try {
            CopyManager copyManager = new CopyManager((BaseConnection) conn);

            // COPY с настройками для максимальной производительности
            String copySql = "COPY logs_unlogged(time, ip, username, url, status_code, domain, " +
                    "response_time_ms, response_size_bytes, action) " +
                    "FROM STDIN WITH (" +
                    "FORMAT CSV, " +
                    "DELIMITER ',', " +
                    "NULL '\\N', " +
                    "ENCODING 'UTF8', " +
                    "ESCAPE '\\', " +
                    "QUOTE '\"')";

            long startCopyTime = System.currentTimeMillis();
            long rowsImported = copyManager.copyIn(copySql, dataStream, 65536); // 64KB буфер

            long copyTime = System.currentTimeMillis() - startCopyTime;
            System.out.printf("COPY завершен за %.1f секунд. Загружено: %,d строк (%.0f строк/сек)%n",
                    copyTime / 1000.0, rowsImported, rowsImported / (copyTime / 1000.0));

        } catch (Exception e) {
            System.err.println("Ошибка при выполнении COPY: " + e.getMessage());
            throw new RuntimeException("COPY failed", e);
        }
    }

    private void updateProgress(ParsingStatus status, long lineNumber, long totalLines,
                                long totalRecords, long batchStartTime) {
        long currentTime = System.currentTimeMillis();
        double batchTime = (currentTime - batchStartTime) / 1000.0;
        double speed = 100000.0 / batchTime;

        // Обновляем статус
        status.processed = lineNumber;
        status.progress = (lineNumber * 100.0) / totalLines;

        // Периодический вывод статистики
        if (totalRecords % 500000 == 0) {
            System.out.printf("[Прогресс] Обработано: %,d/%,d строк (%.1f%%), " +
                            "Записей: %,d, Скорость: %,.0f строк/сек%n",
                    lineNumber, totalLines, status.progress,
                    totalRecords, speed);
        }
    }

    private void completeProcessing(Connection conn, ParsingStatus status,
                                    long startTime, long totalLines, long totalRecords,
                                    double countingWeight, double parsingWeight,
                                    double finalizationWeight, double indexingWeight,
                                    double statisticsWeight, long parsingDuration)
            throws SQLException, InterruptedException {

        System.out.println("Завершающая обработка данных...");

        // СОХРАНЯЕМ ФАКТИЧЕСКОЕ ВРЕМЯ ПАРСИНГА
        status.parsingDuration = parsingDuration;

        // РАССЧИТЫВАЕМ ОБЩЕЕ ВРЕМЯ НА ОСНОВЕ ВЕСОВ
        long estimatedTotalTime = (long)(parsingDuration / parsingWeight);
        status.estimatedTotalTime = estimatedTotalTime;

        // ЭТАП ФИНАЛИЗАЦИИ
        status.stageName = "🗃️ Финализация таблицы";
        status.stageProgress = 0;

        long finalizationStartTime = System.currentTimeMillis();
        long finalizationEstimatedTime = (long)(estimatedTotalTime * finalizationWeight);
        status.estimatedFinalizationTime = finalizationEstimatedTime;

        try {
            // Запускаем финализацию в отдельном потоке
            Thread finalizationThread = new Thread(() -> {
                try {
                    long actualTime = databaseManager.finalizeTable(conn);
                    status.finalizationDuration = actualTime;

                    // Корректируем коэффициент
                    if (parsingDuration > 0) {
                        status.finalizationFactor = (double) actualTime / parsingDuration;
                    }
                } catch (Exception e) {
                    System.err.println("Ошибка финализации: " + e.getMessage());
                    status.finalizationDuration = System.currentTimeMillis() - finalizationStartTime;
                }
            });

            finalizationThread.start();

            // Мониторинг прогресса финализации
            while (finalizationThread.isAlive()) {
                long elapsedFinalization = System.currentTimeMillis() - finalizationStartTime;

                // ПРОГРЕСС НА ОСНОВЕ ПРОШЕДШЕГО ВРЕМЕНИ
                int stageProgress = (int) Math.min(99, (elapsedFinalization * 100) / finalizationEstimatedTime);
                status.stageProgress = stageProgress;

                // ПЕРЕСЧИТЫВАЕМ ОСТАВШЕЕСЯ ВРЕМЯ ЭТАПА
                long remainingTime = Math.max(0, finalizationEstimatedTime - elapsedFinalization);
                status.estimatedFinalizationTime = remainingTime;

                // ОБЩИЙ ПРОГРЕСС
                double overallProgress = (countingWeight + parsingWeight) * 100 +
                        (finalizationWeight * 100 * stageProgress / 100.0);
                status.progress = (int) overallProgress;

                Thread.sleep(500);
            }

        } catch (Exception e) {
            System.err.println("Ошибка при финализации: " + e.getMessage());
            status.finalizationDuration = System.currentTimeMillis() - finalizationStartTime;
        }

        // ФИНАЛИЗАЦИЯ ЗАВЕРШЕНА
        status.stageProgress = 100;
        status.estimatedFinalizationTime = 0; // Время этапа = 0
        status.progress = (int)((countingWeight + parsingWeight + finalizationWeight) * 100);

        // ЭТАП ИНДЕКСАЦИИ
        status.stageName = "📈 Создание индексов";
        status.stageProgress = 0;

        long indexingStartTime = System.currentTimeMillis();
        long indexingEstimatedTime = (long)(status.estimatedTotalTime * indexingWeight);
        status.estimatedIndexingTime = indexingEstimatedTime;

        try {
            Thread indexingThread = new Thread(() -> {
                try {
                    long actualTime = databaseManager.createIndexesWithProgressTracking(conn, (progress) -> {
                        status.stageProgress = progress;

                        // ПЕРЕСЧИТЫВАЕМ ОСТАВШЕЕСЯ ВРЕМЯ
                        long elapsedIndexing = System.currentTimeMillis() - indexingStartTime;
                        long remainingTime = Math.max(0, indexingEstimatedTime - elapsedIndexing);
                        status.estimatedIndexingTime = remainingTime;

                        double overallProgress = (countingWeight + parsingWeight + finalizationWeight) * 100 +
                                (indexingWeight * 100 * progress / 100.0);
                        status.progress = (int) overallProgress;
                    });
                    status.indexingDuration = actualTime;

                    if (parsingDuration > 0) {
                        status.indexingFactor = (double) actualTime / parsingDuration;
                    }
                } catch (Exception e) {
                    System.err.println("Ошибка индексации: " + e.getMessage());
                    status.indexingDuration = System.currentTimeMillis() - indexingStartTime;
                }
            });

            indexingThread.start();
            indexingThread.join();

        } catch (Exception e) {
            System.err.println("Ошибка при индексации: " + e.getMessage());
            status.indexingDuration = System.currentTimeMillis() - indexingStartTime;
        }

        status.stageProgress = 100;
        status.estimatedIndexingTime = 0;
        status.progress = (int)((countingWeight + parsingWeight + finalizationWeight + indexingWeight) * 100);

        // ЭТАП СТАТИСТИКИ
        status.stageName = "📊 Обновление статистики";
        status.stageProgress = 0;

        long statsStartTime = System.currentTimeMillis();
        long statsEstimatedTime = (long)(status.estimatedTotalTime * statisticsWeight);
        status.estimatedStatisticsTime = statsEstimatedTime;

        try {
            Thread statsThread = new Thread(() -> {
                try {
                    aggregatedStatsService.calculateAndSaveDefaultStats();
                    precalculatedTopService.updatePrecalculatedTops();

                    long actualTime = System.currentTimeMillis() - statsStartTime;
                    status.statisticsDuration = actualTime;

                    if (parsingDuration > 0) {
                        status.statisticsFactor = (double) actualTime / parsingDuration;
                    }
                } catch (Exception e) {
                    System.err.println("Ошибка статистики: " + e.getMessage());
                    status.statisticsDuration = System.currentTimeMillis() - statsStartTime;
                }
            });

            statsThread.start();

            while (statsThread.isAlive()) {
                long elapsedStats = System.currentTimeMillis() - statsStartTime;
                int stageProgress = (int) Math.min(99, (elapsedStats * 100) / statsEstimatedTime);

                status.stageProgress = stageProgress;
                status.estimatedStatisticsTime = Math.max(0, statsEstimatedTime - elapsedStats);

                double overallProgress = (countingWeight + parsingWeight + finalizationWeight + indexingWeight) * 100 +
                        (statisticsWeight * 100 * stageProgress / 100.0);
                status.progress = (int) overallProgress;

                Thread.sleep(500);
            }

        } catch (Exception e) {
            System.err.println("Ошибка статистики: " + e.getMessage());
            status.statisticsDuration = System.currentTimeMillis() - statsStartTime;
        }

        // ФИНАЛ
        status.stageProgress = 100;
        status.estimatedStatisticsTime = 0;
        status.progress = 100;
        status.isParsing = false;
        status.stageName = "✅ Завершено";
        status.status = String.format(
                "Парсинг завершен за %.1f мин\n" +
                        "Парсинг: %.1f сек | Финализация: %.1f сек | Индексация: %.1f сек | Статистика: %.1f сек",
                (System.currentTimeMillis() - startTime) / 60000.0,
                parsingDuration / 1000.0,
                status.finalizationDuration / 1000.0,
                status.indexingDuration / 1000.0,
                status.statisticsDuration / 1000.0
        );
    }

    private void finishWithNoData(ParsingStatus status) {
        status.isParsing = false;
        status.status = "❌ Не удалось добавить записи в БД";
        status.progress = 100;
        System.out.println("Парсинг завершен без данных");
    }

    private void handleParsingError(ParsingStatus status, Exception e) {
        System.err.println("Критическая ошибка парсинга: " + e.getMessage());
        e.printStackTrace();

        status.isParsing = false;
        status.status = "❌ Критическая ошибка: " +
                (e.getMessage().length() > 100 ?
                        e.getMessage().substring(0, 100) + "..." :
                        e.getMessage());
        status.progress = 100;

        throw new RuntimeException("Ошибка парсинга", e);
    }

    private long countLines(String filePath, ParsingStatus currentStatus) throws Exception {
        long lines = 0;
        try (BufferedReader br = new BufferedReader(new java.io.FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                lines++;

                // ПРОВЕРКА ОТМЕНЫ КАЖДЫЕ 1000 СТРОК
                if (lines % 1000 == 0 && currentStatus.isCancelled) {
                    System.out.println("Подсчет строк отменен пользователем на строке " + lines);
                    return lines; // Возвращаем текущее количество
                }

                if (lines % 1000000 == 0) {
                    System.out.println("Подсчитано строк: " + lines);

                    // Также обновляем прогресс подсчета строк в статусе
                    if (currentStatus != null) {
                        currentStatus.processed = lines;
                        currentStatus.status = "Подсчет строк: " + lines;
                    }
                }
            }
        }
        return lines;
    }


    private boolean shouldParseLogs(Connection conn, String filePath) {
        System.out.println("Быстрая проверка актуальности...");

        // 1. Проверяем есть ли данные в БД
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM logs")) {

            if (rs.next()) {
                long count = rs.getLong("cnt");
                if (count == 0) {
                    System.out.println("БД пустая - нужно парсить");
                    return true;
                }
            }
        } catch (SQLException e) {
            // Если произошла ошибка при проверке (например, таблицы нет)
            System.out.println("Ошибка проверки БД, считаем что нужно парсить: " + e.getMessage());
            return true;
        }

        // 2. Быстро проверяем первую запись из БД и первую из файла
        try {
            // Получаем дату первой записи из БД
            LocalDateTime dbFirstDate = null;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT MIN(time) as min_time FROM logs")) {
                if (rs.next()) {
                    Timestamp ts = rs.getTimestamp("min_time");
                    if (ts != null) {
                        dbFirstDate = ts.toLocalDateTime();
                        System.out.println("Минимальная дата в БД: " + dbFirstDate);
                    }
                }
            }

            if (dbFirstDate == null) {
                return true;
            }

            // Быстро читаем начало файла
            try (BufferedReader br = new BufferedReader(new java.io.FileReader(filePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    try {
                        java.util.regex.Matcher m = LOG_PATTERN.matcher(line);
                        if (m.find()) {
                            String rawTime = m.group(1);
                            LocalDateTime fileFirstDate = logParserUtils.convertTimestamp(rawTime);

                            if (fileFirstDate != null) {
                                System.out.println("Первая дата в файле: " + fileFirstDate);

                                // Сравниваем только год и месяц
                                boolean samePeriod = dbFirstDate.getYear() == fileFirstDate.getYear() &&
                                        dbFirstDate.getMonthValue() == fileFirstDate.getMonthValue();

                                System.out.println("Данные актуальны (год+месяц совпадают)? " + samePeriod);
                                return !samePeriod; // Если совпадают - не парсить
                            }
                        }
                    } catch (Exception e) {
                        continue;
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("Ошибка при проверке актуальности: " + e.getMessage());
        }

        // В случае ошибки - парсим заново
        return true;
    }

    private String parseLineToCSV(String line) {
        try {
            java.util.regex.Matcher m = LOG_PATTERN.matcher(line);

            if (m.find()) {
                String rawTime = m.group(1);
                int responseTimeMs = Integer.parseInt(m.group(2));
                String ip = m.group(3);
                String action = m.group(4);
                String statusStr = m.group(5);
                long responseSizeBytes = Long.parseLong(m.group(6));
                String username = m.group(9);

                // Пропускаем если username невалидный
                if (username == null || username.equals("-") ||
                        !logParserUtils.isValidUsername(username.trim())) {
                    return null;
                }

                // Определяем statusCode
                int statusCode = parseStatusCode(statusStr, action);

                // Конвертируем время
                LocalDateTime dateTime = logParserUtils.convertTimestamp(rawTime);
                if (dateTime == null) {
                    return null;
                }

                // Извлекаем URL и домен
                String url = m.group(8);
                String domain = logParserUtils.extractDomain(url);

                // Формируем CSV строку
                return formatAsCSV(
                        Timestamp.valueOf(dateTime),
                        ip,
                        username.trim(),
                        url,
                        statusCode,
                        domain != null ? domain : "",
                        responseTimeMs,
                        responseSizeBytes,
                        action
                );
            }
        } catch (Exception e) {
            // Пропускаем некорректные строки
        }

        return null;
    }

    /**
     * Форматирование в CSV
     */
    private String formatAsCSV(Object... values) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(',');

            Object value = values[i];
            if (value == null) {
                sb.append("\\N");
            } else {
                String str = value.toString();

                // Экранирование для CSV
                if (str.contains(",") || str.contains("\"") || str.contains("\n") || str.contains("\r")) {
                    sb.append('"').append(str.replace("\"", "\"\"")).append('"');
                } else {
                    sb.append(str);
                }
            }
        }

        return sb.toString();
    }

    /**
     * Парсинг кода статуса
     */
    private int parseStatusCode(String statusStr, String action) {
        try {
            if (statusStr != null && !statusStr.isEmpty()) {
                return Integer.parseInt(statusStr);
            }

            // Эвристики для определения статуса по action
            if (action.contains("DENIED") || action.contains("DENY")) {
                return 403;
            } else if (action.contains("MISS") || action.contains("HIT") ||
                    action.contains("TUNNEL") || action.contains("REFRESH")) {
                return 200;
            }
        } catch (Exception e) {
            // ignore
        }

        return 0;
    }

    
}