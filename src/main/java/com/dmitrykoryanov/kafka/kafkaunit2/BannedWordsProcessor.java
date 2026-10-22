package com.dmitrykoryanov.kafka.kafkaunit2;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

// Обновляет глобальное хранилище запрещённых слов по записям из топика banned_words.
// При восстановлении Kafka Streams копирует записи топика в глобальное хранилище напрямую, минуя этот процессор.
// Поэтому процессор ничего не преобразует, а только повторяет топик: запись добавляет слово, tombstone удаляет
@Slf4j
public class BannedWordsProcessor implements Processor<String, String, Void, Void> {

    private KeyValueStore<String, String> bannedWordsStore;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        bannedWordsStore = context.getStateStore(MessageTopology.BANNED_WORDS_STORE);
    }

    @Override
    public void process(Record<String, String> record) {
        String word = record.key();
        if (word == null) {
            return;
        }

        if (record.value() == null) {
            bannedWordsStore.delete(word);
            log.info("Слово «{}» удалено из списка запрещённых", word);
        } else {
            bannedWordsStore.put(word, record.value());
            log.info("Слово «{}» добавлено в список запрещённых", word);
        }
    }
}
