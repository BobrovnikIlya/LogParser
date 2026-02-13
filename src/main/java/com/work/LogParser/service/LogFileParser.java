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
    private FilterCacheService filterCacheService;

    @Autowired
    private LogParserUtils logParserUtils;
    @Autowired
    private PrecalculatedTopService precalculatedTopService;

    @Autowired
    private AggregatedStatsService aggregatedStatsService;

    private static final int MEMORY_BUFFER_SIZE = 100 * 1024 * 1024; // 100 MB
    private static final int COPY_BUFFER_SIZE = 64 * 1024; // 64 KB для COPY
    private static final int BATCH_COMMIT_SIZE = 100000; // 100K записей на транзакцию

    private Thread copyThread;
    private PipedOutputStream pos;
    private PipedInputStream pis;
    private BufferedReader reader;
    private volatile boolean cleanupDone = false;

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
        long parsingStageDuration = 0;

        final double COUNTING_WEIGHT = 0.0044;
        final double PARSING_WEIGHT = 386.5 / 1226.5;
        final double FINALIZATION_WEIGHT = 450 / 1226.5;
        final double INDEXING_WEIGHT = 220 / 1226.5;
        final double STATISTICS_WEIGHT = 170 / 1226.5;

        System.out.println("Начало гибридного парсинга с оптимизацией...");

        // Сбрасываем флаги очистки
        cleanupDone = false;
        copyThread = null;
        pos = null;
        pis = null;
        reader = null;

        try (Connection conn = DriverManager.getConnection(
                DatabaseConfig.DB_URL,
                DatabaseConfig.DB_USERNAME,
                DatabaseConfig.DB_PASSWORD)) {

            // Устанавливаем таймаут на соединение
            conn.setNetworkTimeout(null, 60000); // 60 секунд

            currentStatus.parsingSpeed = 1000;
            currentStatus.parsingStageStartTime = System.currentTimeMillis();
            currentStatus.lastProgressUpdateTime = System.currentTimeMillis();
            currentStatus.lastProcessedCount = 0;

            // 1. Подготовка БД
            databaseManager.ensureLogsTableExists(conn);
            databaseManager.createStatusesTable(conn);
            databaseManager.createActionsTable(conn);

            // Проверка отмены перед началом
            if (currentStatus.isCancelled) {
                System.out.println("Парсинг отменен до начала");
                finishWithCancellation(currentStatus);
                return;
            }

            if (!shouldParseLogs(conn, filePath)) {
                System.out.println("Парсинг не требуется");
                currentStatus.isParsing = false;
                currentStatus.progress = 100;
                currentStatus.stageProgress = 100;
                currentStatus.stageName = "Готово";
                return;
            }

            // ===== ПОДСЧЕТ СТРОК =====
            currentStatus.stageName = "📊 Подсчет строк";
            currentStatus.stageProgress = 0;
            currentStatus.progress = 0;
            currentStatus.status = "Подсчет строк...";

            // Запускаем подсчет строк с поддержкой отмены
            final long[] lineCount = new long[1];
            Thread countThread = new Thread(() -> {
                try {
                    lineCount[0] = estimateLineCountWithNIO(filePath);
                } catch (Exception e) {
                    if (!currentStatus.isCancelled) {
                        System.err.println("Ошибка подсчета строк: " + e.getMessage());
                    }
                    lineCount[0] = 1000000;
                }
            });

            countThread.start();

            // Ждем максимум 10 секунд с проверкой отмены
            try {
                long waitStart = System.currentTimeMillis();
                while (countThread.isAlive() && !currentStatus.isCancelled) {
                    if (System.currentTimeMillis() - waitStart > 10000) {
                        countThread.interrupt();
                        break;
                    }
                    Thread.sleep(100);
                }
                if (currentStatus.isCancelled) {
                    countThread.interrupt();
                    finishWithCancellation(currentStatus);
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            totalLines = lineCount[0] > 0 ? lineCount[0] : 1000000;
            currentStatus.total = totalLines;

            currentStatus.stageProgress = 100;
            currentStatus.progress = (int) (COUNTING_WEIGHT * 100);
            currentStatus.status = "Подсчет строк завершен: " + String.format("%,d", totalLines) + " строк";

            // Проверка отмены после подсчета строк
            if (currentStatus.isCancelled) {
                finishWithCancellation(currentStatus);
                return;
            }

            // 3. Очистка и создание таблицы
            databaseManager.clearLogsTable(conn);
            databaseManager.createUnloggedTable(conn);

            // 4. Оптимизация настроек БД перед COPY
            databaseManager.prepareConnectionForCopy(conn);

            // 5. Гибридная загрузка
            System.out.println("Начало гибридной загрузки с оптимизированным чтением...");
            currentStatus.stageName = "🚀 Парсинг данных";
            currentStatus.parsingStageStartTime = System.currentTimeMillis();

            // Создаем Piped потоки с таймаутом
            pos = new PipedOutputStream();
            pis = new PipedInputStream(pos, MEMORY_BUFFER_SIZE);

            // Запускаем COPY в отдельном потоке
            final InputStream dataStreamForCopy = pis;
            copyThread = new Thread(() -> {
                performStreamingCopyWithOptimization(conn, dataStreamForCopy, currentStatus);
            });
            copyThread.start();

            // Основной поток: парсинг с оптимизированным чтением
            try {
                reader = createOptimizedReader(filePath);
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(pos, StandardCharsets.UTF_8),
                        COPY_BUFFER_SIZE);

                String line;
                long lineNumber = 0;
                long recordsInBatch = 0;

                while ((line = reader.readLine()) != null) {
                    lineNumber++;

                    // Проверка отмены на каждой итерации
                    if (currentStatus.isCancelled) {
                        System.out.println("🚫 Парсинг прерван пользователем на строке " + lineNumber);
                        writer.flush();
                        writer.close();
                        pos.close();
                        break;
                    }

                    String csvLine = parseLineToCSV(line);
                    if (csvLine != null) {
                        writer.write(csvLine);
                        writer.write('\n');
                        totalRecords++;
                        recordsInBatch++;

                        if (lineNumber % 5000 == 0) {
                            currentStatus.processed = lineNumber;

                            long currentTime = System.currentTimeMillis();
                            if (currentStatus.parsingStageStartTime > 0) {
                                long elapsedSeconds = (currentTime - currentStatus.parsingStageStartTime) / 1000;
                                if (elapsedSeconds > 0) {
                                    currentStatus.parsingSpeed = (double) lineNumber / elapsedSeconds;
                                }
                            }

                            double stageProgress = (lineNumber * 100.0) / totalLines;
                            double overallProgress = COUNTING_WEIGHT * 100 +
                                    (PARSING_WEIGHT * 100 * stageProgress / 100.0);

                            currentStatus.stageProgress = (int) stageProgress;
                            currentStatus.progress = (int) overallProgress;
                        }
                    }
                }

                // Если не было отмены, финализируем запись
                if (!currentStatus.isCancelled) {
                    writer.flush();
                    writer.close();
                }

                // Замеряем время парсинга
                parsingStageDuration = System.currentTimeMillis() - currentStatus.parsingStageStartTime;
                System.out.println("Парсинг завершен за " + (parsingStageDuration / 1000.0) + " сек");

                currentStatus.actualParsingTime = parsingStageDuration;
                currentStatus.parsingDuration = parsingStageDuration;
                currentStatus.parsingCompleted = !currentStatus.isCancelled;

                if (!currentStatus.isCancelled) {
                    currentStatus.stageProgress = 100;
                    currentStatus.progress = (int) (COUNTING_WEIGHT * 100 + PARSING_WEIGHT * 100);
                }

            } catch (IOException e) {
                if (currentStatus.isCancelled) {
                    System.out.println("Парсинг отменен, игнорируем ошибку ввода-вывода");
                } else {
                    System.err.println("Ошибка при чтении/записи: " + e.getMessage());
                    throw e;
                }
            } finally {
                // Закрываем ресурсы
                try {
                    if (reader != null) reader.close();
                } catch (IOException ignored) {
                }
            }

            // Ждем завершения COPY с таймаутом
            if (copyThread != null && copyThread.isAlive()) {
                copyThread.join(30000); // Максимум 30 секунд ожидания
                if (copyThread.isAlive()) {
                    copyThread.interrupt();
                    System.out.println("⚠️ COPY поток не отвечает, принудительное завершение");
                }
            }

            // Если была отмена, не выполняем дальнейшие этапы
            if (currentStatus.isCancelled) {
                finishWithCancellation(currentStatus);
                return;
            }

            // 6. Восстановление настроек БД
            databaseManager.restoreConnectionSettings(conn);

            // 7. Финализация если есть данные
            if (totalRecords > 0 && !currentStatus.isCancelled) {
                completeProcessing(conn, currentStatus, startTime, totalLines, totalRecords,
                        COUNTING_WEIGHT, PARSING_WEIGHT, FINALIZATION_WEIGHT,
                        INDEXING_WEIGHT, STATISTICS_WEIGHT, parsingStageDuration);
            } else {
                finishWithNoData(currentStatus);
            }

        } catch (Exception e) {
            if (!currentStatus.isCancelled) {
                handleParsingError(currentStatus, e);
            } else {
                System.out.println("Парсинг отменен, ошибка игнорируется: " + e.getMessage());
                finishWithCancellation(currentStatus);
            }
        } finally {
            cleanup();
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
        final InputStream localDataStream = dataStream;

        try {
            // Проверка отмены перед началом
            if (status.isCancelled || Thread.currentThread().isInterrupted()) {
                System.out.println("COPY отменен перед запуском");
                try {
                    localDataStream.close();
                } catch (IOException ignored) {
                }
                return;
            }

            // Устанавливаем таймаут на соединение
            if (conn instanceof BaseConnection) {
                ((BaseConnection) conn).setNetworkTimeout(null, 30000); // 30 секунд
            }

            CopyManager copyManager = new CopyManager((BaseConnection) conn);

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

            // Запускаем COPY с возможностью прерывания
            final Exception[] copyError = new Exception[1];
            final long[] rowsImported = new long[1];

            Thread copyExecutor = new Thread(() -> {
                try {
                    rowsImported[0] = copyManager.copyIn(copySql, localDataStream, 65536);
                } catch (Exception e) {
                    copyError[0] = e;
                }
            });

            copyExecutor.start();

            // Мониторим выполнение COPY с проверкой отмены
            while (copyExecutor.isAlive()) {
                if (status.isCancelled || Thread.currentThread().isInterrupted()) {
                    System.out.println("🚫 COPY отменен пользователем");
                    copyExecutor.interrupt();

                    // Прерываем операцию COPY через БД
                    try {
                        Statement stmt = conn.createStatement();
                        stmt.execute("SELECT pg_cancel_backend(pg_backend_pid())");
                        stmt.close();
                    } catch (SQLException ignored) {
                    }

                    copyExecutor.join(5000);
                    if (copyExecutor.isAlive()) {
                        copyExecutor.interrupt(); // deprecated, но как крайняя мера
                    }
                    return;
                }
                Thread.sleep(100);
            }

            // Если произошла ошибка и это не отмена
            if (copyError[0] != null && !status.isCancelled) {
                throw copyError[0];
            }

            long copyTime = System.currentTimeMillis() - startCopyTime;
            if (!status.isCancelled) {
                System.out.printf("COPY завершен за %.1f секунд. Загружено: %,d строк (%.0f строк/сек)%n",
                        copyTime / 1000.0, rowsImported[0], rowsImported[0] / (copyTime / 1000.0));
            }

        } catch (Exception e) {
            if (!status.isCancelled) {
                System.err.println("Ошибка при выполнении COPY: " + e.getMessage());
                throw new RuntimeException("COPY failed", e);
            }
        } finally {
            try {
                localDataStream.close();
            } catch (IOException ignored) {
            }
        }
    }

    private void finishWithCancellation(ParsingStatus status) {
        status.isParsing = false;
        status.isCancelled = true;
        status.status = "🚫 Парсинг отменен пользователем";
        status.progress = 0;
        status.stageProgress = 0;
        status.stageName = "Отменено";
        status.estimatedTimeRemaining = 0;

        System.out.println("🚫 Парсинг отменен пользователем");
        cleanup();
    }

    public void cleanup() {
        if (cleanupDone) return;
        cleanupDone = true;

        System.out.println("🧹 Очистка ресурсов парсинга...");

        // Закрываем reader
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException ignored) {
            }
            reader = null;
        }

        // Закрываем выходной поток
        if (pos != null) {
            try {
                pos.close();
            } catch (IOException ignored) {
            }
            pos = null;
        }

        // Закрываем входной поток
        if (pis != null) {
            try {
                pis.close();
            } catch (IOException ignored) {
            }
            pis = null;
        }

        // Прерываем COPY поток
        if (copyThread != null && copyThread.isAlive()) {
            copyThread.interrupt();
            try {
                copyThread.join(5000);
            } catch (InterruptedException ignored) {
            }
            copyThread = null;
        }

        System.out.println("✅ Очистка ресурсов завершена");
    }

    private void updateProgress(ParsingStatus status, long lineNumber, long totalLines,
                                long totalRecords, long batchStartTime) {
        long currentTime = System.currentTimeMillis();
        double batchTime = (currentTime - batchStartTime) / 1000.0;
        double speed = 100000.0 / batchTime;

        // ОБНОВЛЯЕМ ПОЛЯ ДЛЯ РАСЧЕТА СКОРОСТИ
        status.processed = lineNumber;
        status.progress = (lineNumber * 100.0) / totalLines;

        // Сохраняем скорость обработки
        if (speed > 0 && speed < 1000000) { // Разумные значения
            status.parsingSpeed = speed;
        }

        // Обновляем время последнего прогресса каждые 2 секунды
        if (currentTime - status.lastProgressUpdateTime > 2000) {
            status.lastProgressUpdateTime = currentTime;
            status.lastProcessedCount = lineNumber;
        }

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

        // Константы весов этапов
        final double PARSING_WEIGHT = 386.5;
        final double FINALIZATION_WEIGHT = 450.0;
        final double INDEXING_WEIGHT = 220.0;
        final double STATISTICS_WEIGHT = 170.0;

        // ✅ ПРОВЕРКА ОТМЕНЫ
        if (status.isCancelled) {
            System.out.println("🚫 Завершающая обработка отменена");
            throw new InterruptedException("Отменено пользователем");
        }

        // ✅ ИСПРАВЛЕНИЕ: Расчет времени этапов на основе времени парсинга
        status.actualParsingTime = parsingDuration;
        status.parsingDuration = parsingDuration;
        status.parsingCompleted = true;

        // Время остальных этапов = время парсинга * вес этапа / вес парсинга
        status.estimatedFinalizationTime = (long) (parsingDuration * (FINALIZATION_WEIGHT / PARSING_WEIGHT));
        status.estimatedIndexingTime = (long) (parsingDuration * (INDEXING_WEIGHT / PARSING_WEIGHT));
        status.estimatedStatisticsTime = (long) (parsingDuration * (STATISTICS_WEIGHT / PARSING_WEIGHT));

        // ✅ ИСПРАВЛЕНИЕ: Общее оставшееся время = сумма времен всех незавершенных этапов
        status.estimatedTimeRemaining = status.estimatedFinalizationTime +
                status.estimatedIndexingTime +
                status.estimatedStatisticsTime;

        System.out.printf("📊 Оценка времени этапов (на основе парсинга: %.1f сек):\n", parsingDuration / 1000.0);
        System.out.printf("   - Финализация: %.1f сек\n", status.estimatedFinalizationTime / 1000.0);
        System.out.printf("   - Индексация: %.1f сек\n", status.estimatedIndexingTime / 1000.0);
        System.out.printf("   - Статистика: %.1f сек\n", status.estimatedStatisticsTime / 1000.0);
        System.out.printf("   - Всего осталось: %.1f сек\n", status.estimatedTimeRemaining / 1000.0);

        long currentTime = System.currentTimeMillis();

        // === ЭТАП ФИНАЛИЗАЦИИ ===
        // ✅ ПРОВЕРКА ОТМЕНЫ
        if (status.isCancelled) {
            System.out.println("🚫 Финализация отменена");
            throw new InterruptedException("Отменено пользователем");
        }

        status.stageStartTime = System.currentTimeMillis();
        status.stageName = "🗃️ Финализация таблицы";
        status.stageProgress = 0;

        AtomicBoolean finalizationCompleted = new AtomicBoolean(false);
        AtomicLong actualFinalizationTime = new AtomicLong(0);

        Thread finalizationThread = new Thread(() -> {
            try {
                long finalizationStartTime = System.currentTimeMillis();
                databaseManager.finalizeTable(conn, null, status);
                long finalizationEndTime = System.currentTimeMillis();
                actualFinalizationTime.set(finalizationEndTime - finalizationStartTime);
                finalizationCompleted.set(true);
                System.out.println("Финализация выполнена за " + (actualFinalizationTime.get() / 1000.0) + " сек");
            } catch (InterruptedException e) {
                System.out.println("🚫 Финализация прервана: " + e.getMessage());
                finalizationCompleted.set(false);
            } catch (Exception e) {
                System.err.println("Ошибка при финализации: " + e.getMessage());
                finalizationCompleted.set(true);
            }
        });

        finalizationThread.start();

        // Мониторинг финализации с проверкой отмены
        long finalizationStartTime = System.currentTimeMillis();
        while (finalizationThread.isAlive()) {
            // ✅ ПРОВЕРКА ОТМЕНЫ
            if (status.isCancelled) {
                System.out.println("🚫 Отмена во время финализации");
                finalizationThread.interrupt();
                status.estimatedTimeRemaining = status.estimatedIndexingTime + status.estimatedStatisticsTime;
                throw new InterruptedException("Отменено пользователем");
            }

            long elapsedFinalizationTime = System.currentTimeMillis() - finalizationStartTime;
            double stageProgress = Math.min(99.0, (elapsedFinalizationTime * 100.0) /
                    Math.max(1, status.estimatedFinalizationTime));

            // ✅ ИСПРАВЛЕНИЕ: Обновляем общее оставшееся время
            long remainingFinalization = (long) (status.estimatedFinalizationTime * (100 - stageProgress) / 100.0);
            status.estimatedTimeRemaining = remainingFinalization +
                    status.estimatedIndexingTime +
                    status.estimatedStatisticsTime;

            double overallProgress = (countingWeight + parsingWeight) * 100 +
                    (finalizationWeight * 100 * stageProgress / 100.0);

            status.stageProgress = (int) stageProgress;
            status.progress = (int) overallProgress;
            status.status = "Финализация таблицы...";

            Thread.sleep(500);
        }

        // ✅ ПРОВЕРКА ОТМЕНЫ
        if (status.isCancelled) {
            System.out.println("🚫 Отмена после финализации");
            throw new InterruptedException("Отменено пользователем");
        }

        // Завершение финализации
        status.stageProgress = 100;
        status.progress = (int)((countingWeight + parsingWeight + finalizationWeight) * 100);
        status.status = "Финализация таблицы завершена";
        status.actualFinalizationTime = actualFinalizationTime.get();
        status.finalizationCompleted = true;

        try {
            filterCacheService.invalidateCacheAfterDataChange();
            System.out.println("🧹 Кэш фильтров очищен после загрузки новых данных");
        } catch (Exception e) {
            System.err.println("⚠ Ошибка при очистке кэша: " + e.getMessage());
        }

        // ✅ ИСПРАВЛЕНИЕ: Обновляем общее время после финализации
        status.estimatedTimeRemaining = status.estimatedIndexingTime + status.estimatedStatisticsTime;

        Thread.sleep(300);
        currentTime = System.currentTimeMillis();

        // === ЭТАП ИНДЕКСАЦИИ ===
        // ✅ ПРОВЕРКА ОТМЕНЫ
        if (status.isCancelled) {
            System.out.println("🚫 Индексация отменена");
            throw new InterruptedException("Отменено пользователем");
        }

        status.stageStartTime = System.currentTimeMillis();
        status.stageName = "📈 Создание индексов";
        status.stageProgress = 0;

        AtomicBoolean indexingCompleted = new AtomicBoolean(false);
        final int[] currentIndexProgress = {0};
        final int totalIndexWeight = 11;
        AtomicInteger currentIndexWeight = new AtomicInteger(0);

        Thread indexingThread = new Thread(() -> {
            try {
                databaseManager.createIndexesWithProgressTracking(conn, (weightProgress) -> {
                    currentIndexWeight.set(weightProgress);
                }, status);
                indexingCompleted.set(true);
            } catch (InterruptedException e) {
                System.out.println("🚫 Индексация прервана: " + e.getMessage());
                indexingCompleted.set(false);
            } catch (Exception e) {
                System.err.println("Ошибка при создании индексов: " + e.getMessage());
                indexingCompleted.set(true);
            }
        });

        indexingThread.start();

        // Мониторинг индексации с проверкой отмены
        long indexingStartTime = System.currentTimeMillis();
        while (indexingThread.isAlive()) {
            // ✅ ПРОВЕРКА ОТМЕНЫ
            if (status.isCancelled) {
                System.out.println("🚫 Отмена во время индексации");
                indexingThread.interrupt();

                // Отменяем операции создания индексов в БД
                try (Statement cancelStmt = conn.createStatement()) {
                    cancelStmt.execute("SELECT pg_cancel_backend(pg_backend_pid())");
                } catch (SQLException ignored) {}

                status.estimatedTimeRemaining = status.estimatedStatisticsTime;
                throw new InterruptedException("Отменено пользователем");
            }

            long elapsedIndexingTime = System.currentTimeMillis() - indexingStartTime;

            double timeBasedProgress = Math.min(99, (elapsedIndexingTime * 100.0) /
                    Math.max(1, status.estimatedIndexingTime));
            double indexBasedProgress = Math.min(99, (currentIndexWeight.get() * 100.0) / totalIndexWeight);
            double stageProgress = Math.min(99, (timeBasedProgress * 0.5) + (indexBasedProgress * 0.5));

            // ✅ ИСПРАВЛЕНИЕ: Обновляем общее оставшееся время
            long remainingIndexing = (long) (status.estimatedIndexingTime * (100 - stageProgress) / 100.0);
            status.estimatedTimeRemaining = remainingIndexing + status.estimatedStatisticsTime;

            double overallProgress = (countingWeight + parsingWeight + finalizationWeight) * 100 +
                    (indexingWeight * 100 * stageProgress / 100.0);

            status.stageProgress = (int)stageProgress;
            status.progress = (int)overallProgress;
            status.status = String.format("Создание индексов... (%d%%)", (int)stageProgress);

            Thread.sleep(1000);
        }

        // ✅ ПРОВЕРКА ОТМЕНЫ
        if (status.isCancelled) {
            System.out.println("🚫 Отмена после индексации");
            throw new InterruptedException("Отменено пользователем");
        }

        // Завершение индексации
        status.stageProgress = 100;
        status.progress = (int)((countingWeight + parsingWeight + finalizationWeight + indexingWeight) * 100);
        status.status = "Создание индексов завершено";
        status.actualIndexingTime = System.currentTimeMillis() - indexingStartTime;
        status.indexingCompleted = true;

        // ✅ ИСПРАВЛЕНИЕ: Обновляем общее время после индексации
        status.estimatedTimeRemaining = status.estimatedStatisticsTime;

        Thread.sleep(500);
        currentTime = System.currentTimeMillis();

        // === ЭТАП СТАТИСТИКИ ===
        // ✅ ПРОВЕРКА ОТМЕНЫ
        if (status.isCancelled) {
            System.out.println("🚫 Статистика отменена");
            throw new InterruptedException("Отменено пользователем");
        }

        status.stageStartTime = System.currentTimeMillis();
        status.stageName = "📊 Обновление статистики";
        status.stageProgress = 0;

        AtomicBoolean statsCompleted = new AtomicBoolean(false);

        Thread statisticsThread = new Thread(() -> {
            try {
                long statsStartTime = System.currentTimeMillis();

                // ✅ ПРОВЕРКА ОТМЕНЫ внутри потока
                if (status.isCancelled) {
                    System.out.println("🚫 Статистика отменена");
                    return;
                }

                databaseManager.updateStatistics(conn);

                // ✅ ПРОВЕРКА ОТМЕНЫ
                if (status.isCancelled) {
                    System.out.println("🚫 Статистика отменена после updateStatistics");
                    return;
                }

                System.out.println("📊 Вычисление и сохранение агрегированной статистики...");
                aggregatedStatsService.calculateAndSaveDefaultStats();

                // ✅ ПРОВЕРКА ОТМЕНЫ
                if (status.isCancelled) {
                    System.out.println("🚫 Статистика отменена после calculateAndSaveDefaultStats");
                    return;
                }

                System.out.println("🔄 Обновление прерассчитанных топов...");
                precalculatedTopService.updatePrecalculatedTops();

                long statsEndTime = System.currentTimeMillis();
                System.out.println("Статистика обновлена за " + ((statsEndTime - statsStartTime) / 1000.0) + " сек");
                statsCompleted.set(true);
            } catch (Exception e) {
                if (!status.isCancelled) {
                    System.err.println("⚠ Ошибка обновления статистики: " + e.getMessage());
                }
                statsCompleted.set(false);
            }
        });

        statisticsThread.start();

        // Мониторинг статистики с проверкой отмены
        long statsStartTime = System.currentTimeMillis();
        while (statisticsThread.isAlive()) {
            // ✅ ПРОВЕРКА ОТМЕНЫ
            if (status.isCancelled) {
                System.out.println("🚫 Отмена во время обновления статистики");
                statisticsThread.interrupt();
                status.estimatedTimeRemaining = 0;
                throw new InterruptedException("Отменено пользователем");
            }

            long elapsedStatsTime = System.currentTimeMillis() - statsStartTime;
            double stageProgress = Math.min(99, (elapsedStatsTime * 100.0) /
                    Math.max(1, status.estimatedStatisticsTime));

            // ✅ ИСПРАВЛЕНИЕ: Обновляем общее оставшееся время
            long remainingStatistics = (long) (status.estimatedStatisticsTime * (100 - stageProgress) / 100.0);
            status.estimatedTimeRemaining = remainingStatistics;

            double overallProgress = (countingWeight + parsingWeight + finalizationWeight + indexingWeight) * 100 +
                    (statisticsWeight * 100 * stageProgress / 100.0);

            status.stageProgress = (int)stageProgress;
            status.progress = (int)overallProgress;
            status.status = String.format("Обновление статистики... (%d%%)", (int)stageProgress);

            Thread.sleep(1000);
        }

        // ✅ ПРОВЕРКА ОТМЕНЫ
        if (status.isCancelled) {
            System.out.println("🚫 Отмена после статистики");
            throw new InterruptedException("Отменено пользователем");
        }

        // Завершение статистики
        status.actualStatisticsTime = System.currentTimeMillis() - statsStartTime;
        status.statisticsCompleted = true;
        status.estimatedTimeRemaining = 0; // ✅ Все этапы завершены

        // Финальный статус
        if (!status.isCancelled && statsCompleted.get()) {
            status.stageProgress = 100;
            status.progress = 100;
            status.isParsing = false;
            status.stageName = "✅ Завершено";
            status.status = String.format(
                    "Парсинг завершен за %.1f мин\n" +
                            "Обработано: %,d строк\n" +
                            "Добавлено: %,d записей\n" +
                            "Средняя скорость: %,.0f записей/сек",
                    (System.currentTimeMillis() - startTime) / 60000.0,
                    totalLines, totalRecords,
                    totalRecords / ((System.currentTimeMillis() - startTime) / 1000.0)
            );
        } else if (status.isCancelled) {
            status.stageProgress = 0;
            status.progress = 0;
            status.isParsing = false;
            status.stageName = "🚫 Отменено";
            status.status = "Парсинг отменен пользователем";
        } else {
            status.stageProgress = 100;
            status.progress = 100;
            status.isParsing = false;
            status.stageName = "⚠ Завершено с ошибками";
            status.status = "Парсинг завершен с ошибками при обновлении статистики";
        }
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