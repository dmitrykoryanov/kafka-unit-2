package com.dmitrykoryanov.kafka.kafkaunit2;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashSet;
import java.util.Set;

// Ведёт списки заблокированных по событиям из топика blocked_users.
// У каждого пользователя свой список (множество тех, кого он заблокировал) в persistent state store.
// Дальше по топологии ничего не передаёт: списки читает BlockFilterProcessor
@Slf4j
public class BlockedUsersProcessor implements Processor<String, BlockEvent, Void, Void> {

    private KeyValueStore<String, Set<String>> blockedUsersStore;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        blockedUsersStore = context.getStateStore(MessageTopology.BLOCKED_USERS_STORE);
    }

    @Override
    public void process(Record<String, BlockEvent> record) {
        String user = record.key();
        BlockEvent event = record.value();
        if (user == null || event == null || event.blockedUser() == null) {
            log.warn("Пропускаем некорректное событие блокировки: ключ={}, значение={}", user, event);
            return;
        }

        Set<String> blockedUsers = new HashSet<>();
        Set<String> stored = blockedUsersStore.get(user);
        if (stored != null) {
            blockedUsers.addAll(stored);
        }

        // Событие без action считаем блокировкой
        if (event.action() == BlockEvent.Action.UNBLOCK) {
            blockedUsers.remove(event.blockedUser());
        } else {
            blockedUsers.add(event.blockedUser());
        }

        if (blockedUsers.isEmpty()) {
            blockedUsersStore.delete(user);
        } else {
            blockedUsersStore.put(user, blockedUsers);
        }
        log.info("Пользователь {}: {} пользователя {}, список заблокированных: {}", user,
                event.action() == BlockEvent.Action.UNBLOCK ? "снятие блокировки" : "блокировка",
                event.blockedUser(), blockedUsers);
    }
}
