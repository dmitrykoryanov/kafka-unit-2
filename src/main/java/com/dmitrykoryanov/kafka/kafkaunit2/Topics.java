package com.dmitrykoryanov.kafka.kafkaunit2;

// Топики приложения. TopicCreator создаёт их при старте, если их ещё нет
public final class Topics {

    // Входящие сообщения. Ключ – получатель, значение – Message в JSON
    public static final String MESSAGES = "messages";
    // Сообщения после блокировки и цензуры, в том же формате
    public static final String FILTERED_MESSAGES = "filtered_messages";
    // События блокировки. Ключ – пользователь, который блокирует, значение – BlockEvent в JSON
    public static final String BLOCKED_USERS = "blocked_users";
    // Запрещённые слова. Ключ – слово в нижнем регистре, любое значение добавляет слово, tombstone (null) удаляет его
    public static final String BANNED_WORDS = "banned_words";

    private Topics() {
    }
}
