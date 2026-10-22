# Образ приложения. Перед сборкой образа соберите jar: ./gradlew bootJar
# Образ на Ubuntu, а не на Alpine: RocksDB (persistent state store) требует glibc
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY build/libs/kafka-unit-2-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
