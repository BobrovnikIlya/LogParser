package com.work.LogParser;

import com.work.LogParser.Log;
import com.work.LogParser.Repositories.LogRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Random;

@Component
public class DataLoader implements CommandLineRunner {

    private final LogRepository logRepository;

    public DataLoader(LogRepository logRepository) {
        this.logRepository = logRepository;
    }

    @Override
    public void run(String... args) throws Exception {
        Random rand = new Random();

        for (int i = 1; i <= 10; i++) {
            Log log = new Log(
                    LocalDateTime.now().minusDays(rand.nextInt(10)), // случайная дата за последние 10 дней
                    "192.168.0." + rand.nextInt(255),
                    "/page" + rand.nextInt(10),
                    "user" + rand.nextInt(5)
            );
            logRepository.save(log);
        }

        System.out.println("10 логов добавлены!");
    }
}
