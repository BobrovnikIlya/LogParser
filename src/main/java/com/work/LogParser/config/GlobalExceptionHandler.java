package com.work.LogParser.config;

import org.apache.catalina.connector.ClientAbortException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.util.Map;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ClientAbortException.class)
    public void handleClientAbortException(ClientAbortException ex) {
        // Просто логируем, что клиент прервал соединение
        System.err.println("Клиент прервал соединение: " + ex.getMessage());
        // Не выбрасываем исключение дальше - это нормальное поведение при отмене запроса
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<?> handleAllExceptions(Exception ex, WebRequest request) {
        // Игнорируем ошибки favicon.ico
        String requestPath = request.getDescription(false);
        if (requestPath.contains("favicon.ico")) {
            return ResponseEntity.notFound().build(); // Просто возвращаем 404
        }

        System.err.println("Глобальный обработчик исключений: " + ex.getMessage());
        ex.printStackTrace();

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of(
                        "success", false,
                        "error", "Внутренняя ошибка сервера: " + ex.getMessage()
                ));
    }

    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<Void> handleNoResourceFoundException(NoResourceFoundException ex) {
        // Игнорируем ошибки для .well-known путей
        if (ex.getMessage() != null && ex.getMessage().contains(".well-known")) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }
}