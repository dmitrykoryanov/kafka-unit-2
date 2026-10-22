package com.dmitrykoryanov.kafka.kafkaunit2;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Set;

// Отбрасывает сообщения от пользователей, которых получатель заблокировал, остальные передаёт на цензуру.
// Ключ сообщения – получатель, поэтому его список блокировок лежит в хранилище той же задачи
@Slf4j
public class BlockFilterProcessor implements Processor<String, Message, String, Message> {

    private ProcessorContext<String, Message> context;
    private KeyValueStore<String, Set<String>> blockedUsersStore;

    @Override
    public void init(ProcessorContext<String, Message> context) {
        this.context = context;
        blockedUsersStore = context.getStateStore(MessageTopology.BLOCKED_USERS_STORE);
    }

    @Override
    public void process(Record<String, Message> record) {
        Message message = record.value();
        if (message == null) {
            return;
        }

        String recipient = record.key();
        Set<String> blockedUsers = recipient == null ? null : blockedUsersStore.get(recipient);
        if (blockedUsers != null && message.from() != null && blockedUsers.contains(message.from())) {
            log.info("Сообщение {} -> {} не доставлено: отправитель в списке блокировок получателя",
                    message.from(), recipient);
            return;
        }
        context.forward(record);
    }
}
