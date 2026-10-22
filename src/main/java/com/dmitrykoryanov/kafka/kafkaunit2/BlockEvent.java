package com.dmitrykoryanov.kafka.kafkaunit2;

// Событие из топика blocked_users. Кто блокирует, указано в ключе записи, а не здесь:
// по ключу Kafka выбирает партицию, и событие попадает в ту же задачу, что и сообщения этому пользователю
public record BlockEvent(String blockedUser, Action action) {

    public enum Action {
        BLOCK,
        UNBLOCK
    }
}
