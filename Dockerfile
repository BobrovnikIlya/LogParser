# ---- Этап 1: Сборка приложения ----
FROM maven:3.9.6-eclipse-temurin-17 AS builder

# Настройки для Windows
ENV MAVEN_OPTS="-Dfile.encoding=UTF-8"

WORKDIR /app

# Копируем pom.xml
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Копируем исходный код
COPY src ./src

# Собираем приложение, полностью отключая тестовый модуль
RUN mvn clean package -DskipTests -Dmaven.test.skip=true -Dfile.encoding=UTF-8

# ---- Этап 2: Запуск ----
FROM eclipse-temurin:17-jre-alpine

ENV JAVA_OPTS="-Dfile.encoding=UTF-8"

RUN apk add --no-cache tzdata

RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

WORKDIR /app

COPY --from=builder /app/target/*.jar app.jar

RUN mkdir -p /app/logs

EXPOSE 8080

ENTRYPOINT ["java", \
    "-Dfile.encoding=UTF-8", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-jar", "app.jar"]