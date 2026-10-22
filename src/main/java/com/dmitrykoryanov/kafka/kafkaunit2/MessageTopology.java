package com.dmitrykoryanov.kafka.kafkaunit2;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import tools.jackson.core.type.TypeReference;

import java.util.Set;

// Топология обработки сообщений на Processor API:
//
//   messages ──> BlockFilterProcessor ──> CensorProcessor ──> filtered_messages
//                      │ читает                │ читает
//              blocked-users-store      banned-words-store (глобальное)
//                      │ пишет                 │ пишет
//   blocked_users ──> BlockedUsersProcessor    BannedWordsProcessor <── banned_words
public final class MessageTopology {

    public static final String BLOCKED_USERS_STORE = "blocked-users-store";
    public static final String BANNED_WORDS_STORE = "banned-words-store";

    private MessageTopology() {
    }

    public static Topology build() {
        Serde<String> stringSerde = Serdes.String();
        Serde<Message> messageSerde = new JsonSerde<>(new TypeReference<Message>() {});
        Serde<BlockEvent> blockEventSerde = new JsonSerde<>(new TypeReference<BlockEvent>() {});
        Serde<Set<String>> blockedUsersSerde = new JsonSerde<>(new TypeReference<Set<String>>() {});

        Topology topology = new Topology();

        // Подзадача 1. Блокировка пользователей
        topology.addSource("BlockedUsersSource",
                stringSerde.deserializer(), blockEventSerde.deserializer(), Topics.BLOCKED_USERS);
        topology.addProcessor("BlockedUsersProcessor", BlockedUsersProcessor::new, "BlockedUsersSource");

        topology.addSource("MessagesSource",
                stringSerde.deserializer(), messageSerde.deserializer(), Topics.MESSAGES);
        topology.addProcessor("BlockFilterProcessor", BlockFilterProcessor::new, "MessagesSource");

        // Persistent state store (RocksDB): пользователь -> множество заблокированных им пользователей.
        // Все изменения пишутся ещё и в changelog-топик, поэтому списки переживают и перезапуск,
        // и потерю локального каталога. Хранилище общее для двух процессоров, поэтому messages и blocked_users
        // должны быть ко-партиционированы: одинаковое число партиций и ключ – пользователь (см. TopicCreator)
        topology.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(BLOCKED_USERS_STORE),
                        stringSerde,
                        blockedUsersSerde),
                "BlockedUsersProcessor", "BlockFilterProcessor");

        // Подзадача 2. Цензура. Глобальное хранилище восстанавливается из самого топика banned_words,
        // поэтому changelog ему не нужен (для глобальных хранилищ Kafka Streams требует его отключить)
        topology.addGlobalStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(BANNED_WORDS_STORE),
                        stringSerde,
                        stringSerde
                ).withLoggingDisabled(),
                "BannedWordsSource",
                stringSerde.deserializer(),
                stringSerde.deserializer(),
                Topics.BANNED_WORDS,
                "BannedWordsProcessor",
                BannedWordsProcessor::new);
        topology.addProcessor("CensorProcessor", CensorProcessor::new, "BlockFilterProcessor");

        topology.addSink("FilteredMessagesSink", Topics.FILTERED_MESSAGES,
                stringSerde.serializer(), messageSerde.serializer(), "CensorProcessor");

        return topology;
    }
}
