# === Stage 1: build ===
FROM maven:3.9-amazoncorretto-21 AS build
WORKDIR /app

COPY pom.xml .
# опционально ускоряем загрузку зависимостей
RUN mvn -B -q -DskipTests dependency:go-offline

# кладём исходники и вспомогательные файлы в build-стадию
COPY src ./src
COPY files ./files
COPY soulway.db ./soulway.db

# собираем jar
RUN mvn -B -DskipTests clean package

# === Stage 2: runtime ===
FROM openjdk:21-jdk-slim
WORKDIR /opt/soulway

# jar
COPY --from=build /app/target/ProfiShinaBot-1.0-SNAPSHOT.jar ./app.jar
# ВАЖНО: переносим материалы и БД в runtime
COPY --from=build /app/files ./files
COPY --from=build /app/soulway.db ./soulway.db

ENV PRODAMUS_WEBHOOK_PORT=8080
EXPOSE 8080
ENTRYPOINT ["java","-jar","/opt/soulway/app.jar"]