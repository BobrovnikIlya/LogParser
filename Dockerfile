# Добавьте в начало файла:
ARG BUILDPLATFORM=linux/amd64

# Используйте для явного указания платформы сборки:
FROM --platform=$BUILDPLATFORM maven:3.9.6-eclipse-temurin-17 AS builder

# ---- Этап 1: Сборка приложения ----
FROM maven:3.9.6-eclipse-temurin-17 AS builder

# Настройки для Windows/Mac/Linux
ENV MAVEN_OPTS="-Dfile.encoding=UTF-8"

WORKDIR /app

# Копируем pom.xml
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Копируем исходный код
COPY src ./src

# Собираем приложение
RUN mvn clean package -DskipTests -Dmaven.test.skip=true -Dfile.encoding=UTF-8

# ---- Этап 2: Запуск ----
FROM eclipse-temurin:17-jre

ENV JAVA_OPTS="-Dfile.encoding=UTF-8"

# Установка tzdata через apt
RUN apt-get update && apt-get install -y tzdata && rm -rf /var/lib/apt/lists/*

# Создание пользователя и группы (для Debian)
RUN groupadd -r spring && useradd -r -g spring spring

# Создаем пользователя и директорию для логов
RUN mkdir -p /app/logs && \
    chown -R spring:spring /app/logs

USER spring:spring

WORKDIR /app

COPY --from=builder /app/target/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", \
    "-Dfile.encoding=UTF-8", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-jar", "app.jar"]