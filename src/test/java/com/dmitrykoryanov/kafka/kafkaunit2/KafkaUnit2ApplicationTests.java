package com.dmitrykoryanov.kafka.kafkaunit2;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class KafkaUnit2ApplicationTests {

    // Заглушка вместо настоящего StreamsRunner: тест не создаёт топики, не запускает Kafka Streams и не требует Kafka
    @MockitoBean
    private StreamsRunner streamsRunner;

    @Test
    void contextLoads() {
    }

}
