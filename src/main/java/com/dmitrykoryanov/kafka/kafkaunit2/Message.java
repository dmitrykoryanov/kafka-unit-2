package com.dmitrykoryanov.kafka.kafkaunit2;

// Сообщение от одного пользователя другому. В топиках хранится в виде JSON
public record Message(String from, String to, String text) {
}
