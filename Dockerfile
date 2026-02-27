# ---- Этап 1: Сборка приложения ----
# Используем образ, совместимый со всеми архитектурами
FROM --platform=$BUILDPLATFORM maven:3.9.6-eclipse-temurin-17 AS builder

# Аргументы для мультиплатформенной сборки
ARG TARGETARCH
ARG BUILDPLATFORM

# Настройки кодировки
ENV MAVEN_OPTS="-Dfile.encoding=UTF-8 -Duser.language=en -Duser.country=US"

WORKDIR /app

# Копируем pom.xml и зависимости
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Копируем исходный код
COPY src ./src

# Собираем приложение
RUN mvn clean package -DskipTests -Dmaven.test.skip=true -Dfile.encoding=UTF-8

# ---- Этап 2: Создание образа с JRE ----
# Используем Alpine Linux для минимального размера и лучшей совместимости
# Alpine доступен для всех архитектур
FROM eclipse-temurin:17-jre

# Метаданные
LABEL maintainer="your-email@example.com" \
      version="1.0" \
      description="Spring Boot application"

# Переменные окружения
ENV JAVA_OPTS="-Dfile.encoding=UTF-8 -Duser.timezone=UTC" \
    TZ=UTC

# Установка tzdata через apt
RUN apt-get update && apt-get install -y tzdata && rm -rf /var/lib/apt/lists/*
# Создание пользователя и группы (для Debian)
RUN groupadd -r spring && useradd -r -g spring spring
USER spring:spring

# Создаем рабочую директорию и устанавливаем права
WORKDIR /app

# Копируем собранное приложение
COPY --from=builder --chown=spring:spring /app/target/*.jar app.jar

# Переключаемся на непривилегированного пользователя
USER spring:spring

# Информация о порте
EXPOSE 8080

# Точка входа
ENTRYPOINT ["java", \
    "-Dfile.encoding=UTF-8", \
    "-Duser.timezone=UTC", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-jar", "app.jar"]

# Проверка работоспособности
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD java -version || exit 1