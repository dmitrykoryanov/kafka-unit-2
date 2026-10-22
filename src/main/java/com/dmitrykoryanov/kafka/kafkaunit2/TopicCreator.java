package com.dmitrykoryanov.kafka.kafkaunit2;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

// Создаёт топики приложения, которых ещё нет. Kafka Streams не запустится, если нет его входных топиков
@Slf4j
@RequiredArgsConstructor
@Component
public class TopicCreator {

    // У messages и blocked_users число партиций должно совпадать (ко-партиционирование, см. MessageTopology)
    private static final int PARTITIONS = 3;
    // В docker-compose.yml один брокер
    private static final short REPLICATION_FACTOR = 1;

    private final KafkaUtils kafkaUtils;

    public void createTopics() throws ExecutionException, InterruptedException {
        List<NewTopic> topics = List.of(
                new NewTopic(Topics.MESSAGES, PARTITIONS, REPLICATION_FACTOR),
                new NewTopic(Topics.FILTERED_MESSAGES, PARTITIONS, REPLICATION_FACTOR),
                // Обычный топик, не компактированный: это журнал событий BLOCK/UNBLOCK, и компактирование
                // оставило бы только последнее событие пользователя
                new NewTopic(Topics.BLOCKED_USERS, PARTITIONS, REPLICATION_FACTOR),
                // Компактированный топик: для каждого слова хранится последняя запись, а tombstone удаляет слово совсем.
                // Глобальное хранилище читает все партиции, поэтому одной достаточно
                new NewTopic(Topics.BANNED_WORDS, 1, REPLICATION_FACTOR)
                        .configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT))
        );

        try (Admin admin = kafkaUtils.getAdminClient()) {
            Set<String> existingTopics = admin.listTopics().names().get();
            List<NewTopic> missingTopics = topics.stream()
                    .filter(topic -> !existingTopics.contains(topic.name()))
                    .toList();
            if (missingTopics.isEmpty()) {
                log.info("Все топики уже существуют");
            } else {
                admin.createTopics(missingTopics).all().get();
                log.info("Созданы топики: {}", missingTopics.stream().map(NewTopic::name).toList());
            }

            // Топики могли появиться раньше с другим числом партиций, например если брокер создал топик
            // автоматически при первой записи. Тогда список блокировок и сообщения попадут в разные задачи
            Map<String, TopicDescription> descriptions =
                    admin.describeTopics(List.of(Topics.MESSAGES, Topics.BLOCKED_USERS)).allTopicNames().get();
            int messagesPartitions = descriptions.get(Topics.MESSAGES).partitions().size();
            int blockedUsersPartitions = descriptions.get(Topics.BLOCKED_USERS).partitions().size();
            if (messagesPartitions != blockedUsersPartitions) {
                throw new IllegalStateException("У топиков " + Topics.MESSAGES + " и " + Topics.BLOCKED_USERS
                        + " разное число партиций: " + messagesPartitions + " и " + blockedUsersPartitions);
            }
        }
    }
}
