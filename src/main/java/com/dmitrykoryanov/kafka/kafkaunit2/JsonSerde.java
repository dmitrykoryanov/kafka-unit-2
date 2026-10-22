package com.dmitrykoryanov.kafka.kafkaunit2;

import org.apache.kafka.common.serialization.Serdes;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

// Serde для объектов в JSON. Тип передаётся через TypeReference, чтобы работали и обобщённые типы,
// например Set<String> в хранилище блокировок.
// Некорректный JSON приводит к JacksonException, и Kafka Streams пропускает такую запись
// (см. DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG в KafkaUtils)
public class JsonSerde<T> extends Serdes.WrapperSerde<T> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public JsonSerde(TypeReference<T> type) {
        super(
                (topic, data) -> data == null ? null : OBJECT_MAPPER.writeValueAsBytes(data),
                (topic, bytes) -> bytes == null ? null : OBJECT_MAPPER.readValue(bytes, type)
        );
    }
}
