package com.work.LogParser.service;

import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.regex.Pattern;

@Service
public class LogParserUtils {

    private static final Pattern USERNAME_PATTERN = Pattern.compile(
            "^(.*_.*_.*|.*user.*)$", Pattern.CASE_INSENSITIVE
    );

    private static final Pattern DOMAIN_PATTERN = Pattern.compile(
            "^(?:https?://)?([^/:]+)(?::\\d+)?(?:/.*)?$"
    );

    public boolean isValidUsername(String username) {
        if (username == null || username.isEmpty()) return false;
        if (username.toLowerCase().contains("user")) return true;

        int underscoreCount = 0;
        for (char c : username.toCharArray()) {
            if (c == '_') underscoreCount++;
        }
        return underscoreCount >= 2;
    }

    public String extractDomain(String url) {
        if (url == null || url.isEmpty() || url.equals("-")) return null;

        try {
            java.util.regex.Matcher matcher = DOMAIN_PATTERN.matcher(url);
            if (matcher.find()) return matcher.group(1);
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    public LocalDateTime convertTimestamp(String rawTimestamp) {
        try {
            if (rawTimestamp.matches("\\d+(\\.\\d+)?")) {
                double epochSeconds = Double.parseDouble(rawTimestamp);
                long seconds = (long) epochSeconds;
                long nanos = (long) ((epochSeconds - seconds) * 1_000_000_000);
                return LocalDateTime.ofInstant(
                        java.time.Instant.ofEpochSecond(seconds, nanos),
                        java.time.ZoneId.systemDefault()
                );
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }
}