package com.dmitrykoryanov.kafka.kafkaunit2;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;

// Spring Boot вызывает run() после старта: создаём топики, запускаем Kafka Streams и, если включено,
// отправляем тестовые данные. Потоки Kafka Streams не демоны, они и держат JVM запущенной
@Slf4j
@RequiredArgsConstructor
@Component
public class StreamsRunner implements CommandLineRunner {

    private static final Duration START_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaUtils kafkaUtils;
    private final TopicCreator topicCreator;
    private final TestDataProducer testDataProducer;

    @Value("${app.test-data.enabled}")
    private boolean testDataEnabled;

    private KafkaStreams streams;

    @Override
    public void run(String... args) throws Exception {
        topicCreator.createTopics();

        Topology topology = MessageTopology.build();
        log.info("Топология: {}", topology.describe());

        streams = new KafkaStreams(topology, kafkaUtils.getStreamsProperties());
        // При необработанной ошибке останавливаем Kafka Streams целиком
        streams.setUncaughtExceptionHandler(exception -> {
            log.error("Необработанная ошибка в Kafka Streams", exception);
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        streams.start();

        if (testDataEnabled) {
            waitUntilRunning();
            testDataProducer.sendTestData();
        }
    }

    // Spring вызывает этот метод при остановке приложения (Ctrl+C, docker stop)
    @PreDestroy
    public void stop() {
        if (streams != null) {
            log.info("Останавливаем Kafka Streams");
            streams.close(CLOSE_TIMEOUT);
        }
    }

    // Ждём, пока Kafka Streams получит партиции и начнёт обработку, иначе паузы между шагами тестовых данных
    // теряют смысл
    private void waitUntilRunning() throws InterruptedException {
        long deadline = System.currentTimeMillis() + START_TIMEOUT.toMillis();
        while (streams.state() != KafkaStreams.State.RUNNING) {
            if (System.currentTimeMillis() > deadline || !streams.state().isRunningOrRebalancing()) {
                throw new IllegalStateException("Kafka Streams не запустился, состояние: " + streams.state());
            }
            Thread.sleep(500);
        }
    }
}
