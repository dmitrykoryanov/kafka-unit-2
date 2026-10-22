package com.dmitrykoryanov.kafka.kafkaunit2;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;

import static com.dmitrykoryanov.kafka.kafkaunit2.BlockEvent.Action.BLOCK;
import static com.dmitrykoryanov.kafka.kafkaunit2.BlockEvent.Action.UNBLOCK;

// Тестовые данные для проверки блокировки и цензуры. Сценарий идёт по шагам с паузами, чтобы Kafka Streams
// успел применить блокировки и список слов до следующих сообщений. Ожидаемый результат указан у каждого сообщения
@Slf4j
@RequiredArgsConstructor
@Component
public class TestDataProducer {

    private static final Duration PAUSE = Duration.ofSeconds(3);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final KafkaUtils kafkaUtils;

    public void sendTestData() throws InterruptedException {
        try (KafkaProducer<String, String> producer = kafkaUtils.getProducer()) {
            log.info("Тестовые данные, шаг 1: запрещённые слова и блокировки");
            addBannedWord(producer, "дурак");
            addBannedWord(producer, "spam");
            addBannedWord(producer, "идиот");
            sendBlockEvent(producer, "Иван", new BlockEvent("Михаил", BLOCK));
            sendBlockEvent(producer, "Анна", new BlockEvent("Мария", BLOCK));
            pause(producer);

            log.info("Тестовые данные, шаг 2: сообщения");
            sendMessage(producer, new Message("Мария", "Иван", "Привет, Иван! Как дела?"));    // Дойдёт без изменений
            sendMessage(producer, new Message("Михаил", "Иван", "Купи наш spam!"));            // Не дойдёт: Михаил в списке Ивана
            sendMessage(producer, new Message("Михаил", "Анна", "Ты дурак, купи наш SPAM"));   // "Ты *****, купи наш ****"
            sendMessage(producer, new Message("Мария", "Анна", "Привет, Анна!"));              // Не дойдёт: Мария в списке Анны
            sendMessage(producer, new Message("Иван", "Мария", "Мария, ты не идиот"));         // "Мария, ты не *****"
            pause(producer);

            log.info("Тестовые данные, шаг 3: меняем список слов и снимаем блокировку");
            addBannedWord(producer, "скидка");
            removeBannedWord(producer, "spam");
            sendBlockEvent(producer, "Анна", new BlockEvent("Мария", UNBLOCK));
            pause(producer);

            log.info("Тестовые данные, шаг 4: сообщения после изменений");
            sendMessage(producer, new Message("Мария", "Анна", "Анна, извини!"));              // Теперь дойдёт
            sendMessage(producer, new Message("Михаил", "Анна", "Огромная скидка на spam"));   // "Огромная ****** на spam"
            sendMessage(producer, new Message("Михаил", "Иван", "Иван, ответь!"));             // По-прежнему не дойдёт
            producer.flush();
        }
        log.info("Тестовые данные отправлены, результат – в топике {}", Topics.FILTERED_MESSAGES);
    }

    // Ключ сообщения – получатель: по нему сообщение попадает в ту же партицию, что и его список блокировок
    private void sendMessage(KafkaProducer<String, String> producer, Message message) {
        send(producer, Topics.MESSAGES, message.to(), OBJECT_MAPPER.writeValueAsString(message));
    }

    private void sendBlockEvent(KafkaProducer<String, String> producer, String user, BlockEvent event) {
        send(producer, Topics.BLOCKED_USERS, user, OBJECT_MAPPER.writeValueAsString(event));
    }

    private void addBannedWord(KafkaProducer<String, String> producer, String word) {
        send(producer, Topics.BANNED_WORDS, word, word);
    }

    // Tombstone: компактированный топик со временем удалит и само слово, и эту запись
    private void removeBannedWord(KafkaProducer<String, String> producer, String word) {
        send(producer, Topics.BANNED_WORDS, word, null);
    }

    private void send(KafkaProducer<String, String> producer, String topic, String key, String value) {
        producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
            if (exception != null) {
                log.error("Не удалось отправить в {}: ключ={}, значение={}", topic, key, value, exception);
            }
        });
        log.info("Отправлено в {}: ключ={}, значение={}", topic, key, value);
    }

    private void pause(KafkaProducer<String, String> producer) throws InterruptedException {
        producer.flush();
        Thread.sleep(PAUSE.toMillis());
    }
}
