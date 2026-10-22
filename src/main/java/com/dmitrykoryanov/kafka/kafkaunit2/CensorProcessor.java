package com.dmitrykoryanov.kafka.kafkaunit2;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Locale;
import java.util.regex.Pattern;

// Заменяет запрещённые слова звёздочками. Список слов лежит в глобальном хранилище: каждая задача видит его целиком,
// а новые записи в топике banned_words применяются на лету, без перезапуска приложения
@Slf4j
public class CensorProcessor implements Processor<String, Message, String, Message> {

    // Слово – непрерывная последовательность букв и цифр любого алфавита
    private static final Pattern WORD = Pattern.compile("[\\p{L}\\p{N}]+");

    private ProcessorContext<String, Message> context;
    private KeyValueStore<String, String> bannedWordsStore;

    @Override
    public void init(ProcessorContext<String, Message> context) {
        this.context = context;
        bannedWordsStore = context.getStateStore(MessageTopology.BANNED_WORDS_STORE);
    }

    @Override
    public void process(Record<String, Message> record) {
        Message message = record.value();
        if (message.text() == null) {
            context.forward(record);
            return;
        }

        // Слова сравниваются без учёта регистра, длина замены совпадает с длиной слова
        String censoredText = WORD.matcher(message.text()).replaceAll(match ->
                isBanned(match.group()) ? "*".repeat(match.group().length()) : match.group());
        if (!censoredText.equals(message.text())) {
            log.info("Сообщение {} -> {} отцензурировано: «{}» -> «{}»",
                    message.from(), record.key(), message.text(), censoredText);
        }
        context.forward(record.withValue(new Message(message.from(), message.to(), censoredText)));
    }

    private boolean isBanned(String word) {
        return bannedWordsStore.get(word.toLowerCase(Locale.ROOT)) != null;
    }
}
