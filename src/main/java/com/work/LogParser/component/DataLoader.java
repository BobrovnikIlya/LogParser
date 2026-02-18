package com.work.LogParser.component;

import com.work.LogParser.service.LogParsingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class DataLoader implements CommandLineRunner {
    @Override
    public void run(String... args) throws Exception {
        // Теперь парсинг запускается только через API
        System.out.println("Приложение запущено. Парсинг доступен через веб-интерфейс.");
    }
}