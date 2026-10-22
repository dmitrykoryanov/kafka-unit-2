package com.dmitrykoryanov.kafka.kafkaunit2;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.core.type.TypeReference;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.dmitrykoryanov.kafka.kafkaunit2.BlockEvent.Action.BLOCK;
import static com.dmitrykoryanov.kafka.kafkaunit2.BlockEvent.Action.UNBLOCK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

// Проверяет топологию без брокера: TopologyTestDriver обрабатывает каждую запись синхронно, сразу при отправке
class MessageTopologyTest {

    @TempDir
    private Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, Message> messages;
    private TestInputTopic<String, BlockEvent> blockedUsers;
    private TestInputTopic<String, String> bannedWords;
    private TestOutputTopic<String, Message> filteredMessages;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "message-filter-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        // Как в KafkaUtils: запись с некорректным JSON пропускается
        props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        driver = new TopologyTestDriver(MessageTopology.build(), props);

        JsonSerde<Message> messageSerde = new JsonSerde<>(new TypeReference<Message>() {});
        JsonSerde<BlockEvent> blockEventSerde = new JsonSerde<>(new TypeReference<BlockEvent>() {});
        messages = driver.createInputTopic(Topics.MESSAGES, new StringSerializer(), messageSerde.serializer());
        blockedUsers = driver.createInputTopic(Topics.BLOCKED_USERS, new StringSerializer(), blockEventSerde.serializer());
        bannedWords = driver.createInputTopic(Topics.BANNED_WORDS, new StringSerializer(), new StringSerializer());
        filteredMessages = driver.createOutputTopic(Topics.FILTERED_MESSAGES,
                new StringDeserializer(), messageSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void messageWithoutBlocksOrBannedWordsIsDeliveredUnchanged() {
        send("Мария", "Иван", "Привет, Иван!");

        assertEquals(List.of(new Message("Мария", "Иван", "Привет, Иван!")), filteredMessages.readValuesToList());
    }

    @Test
    void messageFromBlockedUserIsDropped() {
        blockedUsers.pipeInput("Иван", new BlockEvent("Михаил", BLOCK));

        send("Михаил", "Иван", "Купи!");
        send("Мария", "Иван", "Привет");

        assertEquals(List.of(new Message("Мария", "Иван", "Привет")), filteredMessages.readValuesToList());
    }

    @Test
    void blockAffectsOnlyRecipientWhoBlocked() {
        blockedUsers.pipeInput("Иван", new BlockEvent("Михаил", BLOCK));

        send("Михаил", "Анна", "Привет");

        assertEquals(List.of(new Message("Михаил", "Анна", "Привет")), filteredMessages.readValuesToList());
    }

    @Test
    void unblockedUserCanSendAgain() {
        blockedUsers.pipeInput("Анна", new BlockEvent("Мария", BLOCK));
        send("Мария", "Анна", "Первое");
        blockedUsers.pipeInput("Анна", new BlockEvent("Мария", UNBLOCK));
        send("Мария", "Анна", "Второе");

        assertEquals(List.of(new Message("Мария", "Анна", "Второе")), filteredMessages.readValuesToList());
    }

    @Test
    void eachUserHasOwnBlockListInStore() {
        blockedUsers.pipeInput("Иван", new BlockEvent("Михаил", BLOCK));
        blockedUsers.pipeInput("Иван", new BlockEvent("Ольга", BLOCK));
        blockedUsers.pipeInput("Анна", new BlockEvent("Мария", BLOCK));
        blockedUsers.pipeInput("Пётр", new BlockEvent("Иван", BLOCK));
        blockedUsers.pipeInput("Пётр", new BlockEvent("Иван", UNBLOCK));

        KeyValueStore<String, Set<String>> store = driver.getKeyValueStore(MessageTopology.BLOCKED_USERS_STORE);
        assertEquals(Set.of("Михаил", "Ольга"), store.get("Иван"));
        assertEquals(Set.of("Мария"), store.get("Анна"));
        // Пустой список удаляется из хранилища
        assertNull(store.get("Пётр"));
    }

    @Test
    void bannedWordsAreMaskedIgnoringCase() {
        bannedWords.pipeInput("дурак", "дурак");
        bannedWords.pipeInput("spam", "spam");

        send("Михаил", "Анна", "Ты Дурак, купи наш SPAM! Spammer и дураки не в списке");

        assertEquals(List.of(new Message("Михаил", "Анна", "Ты *****, купи наш ****! Spammer и дураки не в списке")),
                filteredMessages.readValuesToList());
    }

    @Test
    void bannedWordsCanBeAddedAndRemovedOnTheFly() {
        bannedWords.pipeInput("spam", "spam");
        send("Михаил", "Анна", "spam и скидка");

        bannedWords.pipeInput("скидка", "скидка");
        bannedWords.pipeInput("spam", (String) null);  // Tombstone удаляет слово
        send("Михаил", "Анна", "spam и скидка");

        assertEquals(List.of(
                new Message("Михаил", "Анна", "**** и скидка"),
                new Message("Михаил", "Анна", "spam и ******")
        ), filteredMessages.readValuesToList());
    }

    @Test
    void blockedMessageIsNotCensoredAndDeliveredOneIs() {
        blockedUsers.pipeInput("Иван", new BlockEvent("Михаил", BLOCK));
        bannedWords.pipeInput("идиот", "идиот");

        send("Михаил", "Иван", "идиот");
        send("Мария", "Иван", "Не идиот");

        assertEquals(List.of(new Message("Мария", "Иван", "Не *****")), filteredMessages.readValuesToList());
    }

    @Test
    void invalidJsonIsSkipped() {
        TestInputTopic<String, String> rawMessages =
                driver.createInputTopic(Topics.MESSAGES, new StringSerializer(), new StringSerializer());

        rawMessages.pipeInput("Иван", "не JSON");
        send("Мария", "Иван", "Привет");

        assertEquals(List.of(new Message("Мария", "Иван", "Привет")), filteredMessages.readValuesToList());
    }

    // Ключ сообщения – получатель, как и в TestDataProducer
    private void send(String from, String to, String text) {
        messages.pipeInput(to, new Message(from, to, text));
    }
}
