# Kafka Unit 2: блокировка пользователей и цензура сообщений

Учебное приложение на Kafka Streams. Пользователи отправляют друг другу сообщения через топик `messages`. Приложение обрабатывает каждое сообщение в два этапа:

1. Отбрасывает его, если получатель заблокировал отправителя.
2. В остальных сообщениях заменяет запрещённые слова звёздочками и пишет результат в топик `filtered_messages`.

Списки блокировок хранятся в persistent state store. Список запрещённых слов можно менять на лету, без перезапуска приложения.

**Стек:** Java 21, Spring Boot 4.1.1, Gradle 9.8.2, Kafka Streams 4.2.1 (Processor API), Jackson 3, Lombok. Kafka работает в Docker: ZooKeeper и один брокер `confluentinc/cp-kafka:7.0.1`, к ним Kafka UI, Schema Registry и ksqlDB. Приложение тоже запускается в Docker. Schema Registry и ksqlDB оно не использует.

## Логика работы

```
messages ──► BlockFilterProcessor ──► CensorProcessor ──► filtered_messages
                    │ читает                 │ читает
            blocked-users-store       banned-words-store (глобальное)
                    ▲ пишет                  ▲ пишет
blocked_users ──► BlockedUsersProcessor     BannedWordsProcessor ◄── banned_words
```

### Топики

| Топик | Ключ | Значение | Партиции | Назначение |
|---|---|---|---|---|
| `messages` | получатель | `Message` в JSON: `{"from":"Мария","to":"Иван","text":"Привет"}` | 3 | входящие сообщения |
| `filtered_messages` | получатель | `Message` в JSON | 3 | сообщения после блокировки и цензуры |
| `blocked_users` | пользователь, который блокирует | `BlockEvent` в JSON: `{"blockedUser":"Михаил","action":"BLOCK"}`, `action` — `BLOCK` или `UNBLOCK` | 3 | события блокировки и разблокировки |
| `banned_words` | запрещённое слово в нижнем регистре | любое значение добавляет слово, `null` (tombstone) удаляет его | 1, компактированный | список запрещённых слов |

`TopicCreator` создаёт при старте приложения все топики, которых ещё нет. Фактор репликации — 1, потому что брокер один. Топика `banned_words` в задании нет. Он нужен, чтобы список запрещённых слов можно было менять динамически. Кроме этих топиков, Kafka Streams сам создаёт служебный топик `message-filter-app-blocked-users-store-changelog`, это changelog хранилища блокировок.

### Блокировка пользователей (подзадача 1)

**Отдельный список для каждого пользователя.** `BlockedUsersProcessor` читает события из `blocked_users` и ведёт хранилище `blocked-users-store`. В нём ключ — пользователь, а значение — множество пользователей, которых он заблокировал:

- `BLOCK` добавляет пользователя в список;
- `UNBLOCK` убирает его из списка;
- событие без `action` считается блокировкой;
- пустой список удаляется из хранилища.

**Фильтрация.** `BlockFilterProcessor` читает `messages`. Ключ сообщения — получатель, поэтому процессор берёт из хранилища список получателя. Если в этом списке есть отправитель (`from`), сообщение отбрасывается и в лог пишется строка `не доставлено`. Блокировка действует только в одну сторону и только у того, кто блокирует. Например, если Иван заблокировал Михаила, Михаил может писать всем остальным.

**Persistent state store.** Хранилище создаётся через `Stores.persistentKeyValueStore()`, поэтому Kafka Streams хранит его в RocksDB — встраиваемой key-value базе, файлы которой лежат на диске. Каталог задаёт настройка `kafka.state-dir`. В Docker это volume `app-state`, и данные переживают пересоздание контейнера. Каждое изменение хранилища записывается ещё и в changelog-топик. При старте Kafka Streams дочитывает из него изменения, которых нет в локальных файлах. Если локальных файлов нет совсем, хранилище восстанавливается из changelog целиком.

**Ко-партиционирование.** Хранилище общее для двух процессоров. Kafka Streams делит его по партициям: задача, которая обрабатывает партицию N топиков `messages` и `blocked_users`, видит только свою часть хранилища. Поэтому сообщение пользователю и его события блокировки должны попадать в партицию с одним и тем же номером. Для этого выполнены два условия:

- у обоих топиков одинаковый ключ — пользователь: у сообщения это получатель, у события блокировки — тот, кто блокирует;
- у обоих топиков одинаковое число партиций, 3.

Если число партиций разное (например, брокер создал `messages` сам, с одной партицией, при первой записи), `TopicCreator` останавливает приложение с ошибкой.

`blocked_users` — обычный, не компактированный топик. Это журнал событий, а компактирование оставило бы только последнее событие каждого пользователя. Тогда при повторной обработке топика остальные блокировки потерялись бы.

### Цензура (подзадача 2)

**Список слов.** `BannedWordsProcessor` читает `banned_words` и ведёт глобальное хранилище `banned-words-store`. Обычное хранилище делится по партициям, а глобальное каждая задача видит целиком, поэтому ключи сообщений и слов могут не совпадать. Новая запись в `banned_words` добавляет слово, tombstone удаляет его. Изменения применяются сразу, без перезапуска.

Глобальное хранилище восстанавливается из самого топика `banned_words`, отдельного changelog у него нет. Поэтому топик компактированный: в нём остаётся последняя запись для каждого слова. При восстановлении Kafka Streams копирует записи топика в хранилище напрямую, минуя процессор. Поэтому `BannedWordsProcessor` ничего не преобразует и просто повторяет топик.

**Маскирование.** `CensorProcessor` делит текст на слова. Слово — это непрерывная последовательность букв и цифр любого алфавита (`[\p{L}\p{N}]+`). Каждое слово процессор переводит в нижний регистр и ищет в хранилище. Найденное слово заменяется таким же числом звёздочек: `Ты дурак, купи наш SPAM` → `Ты *****, купи наш ****`. Сравниваются только слова целиком. Если запрещены `spam` и `дурак`, то `Spammer` и `дураки` не маскируются.

**Порядок обработки.** Сначала проверяется блокировка, потом цензура. Каждое сообщение, которое доходит до `filtered_messages`, проходит через цензуру, а заблокированные отбрасываются ещё до неё. Ключ сообщения (получатель) при обработке не меняется.

### Сериализация и ошибки

Значения в топиках хранятся в JSON. Их сериализует `JsonSerde`, а ключи — обычные строки. Если запись не удалось прочитать (например, это некорректный JSON), `LogAndContinueExceptionHandler` пишет предупреждение в лог, и Kafka Streams пропускает запись. Без этого обработчика приложение остановилось бы на такой записи. Если при обработке возникнет необработанная ошибка, `StreamsRunner` останавливает Kafka Streams целиком.

### Запуск и остановка

После старта Spring Boot вызывает `StreamsRunner.run()`. Этот метод:

1. создаёт недостающие топики (`TopicCreator`);
2. строит топологию (`MessageTopology.build()`) и пишет её описание в лог;
3. запускает `KafkaStreams`;
4. если включены тестовые данные (`app.test-data.enabled=true`), ждёт до 60 секунд, пока Kafka Streams перейдёт в состояние `RUNNING`, и отправляет тестовый сценарий (`TestDataProducer`).

При остановке приложения Spring вызывает метод `StreamsRunner` с аннотацией `@PreDestroy`. Он закрывает `KafkaStreams` и ждёт до 10 секунд. За это время Kafka Streams фиксирует смещения и сбрасывает хранилища на диск.

## Классы

Все классы находятся в пакете `com.dmitrykoryanov.kafka.kafkaunit2`.

| Класс | Назначение |
|---|---|
| `KafkaUnit2Application` | Точка входа Spring Boot. |
| `StreamsRunner` | Реализует `CommandLineRunner`. После старта создаёт топики, строит топологию, запускает `KafkaStreams` и при необходимости отправляет тестовые данные. При остановке (`@PreDestroy`) закрывает `KafkaStreams`. |
| `TopicCreator` | Создаёт через Admin API топики, которых ещё нет. Проверяет, что у `messages` и `blocked_users` одинаковое число партиций. |
| `Topics` | Имена топиков. В комментариях описан формат ключа и значения каждого топика. |
| `MessageTopology` | Строит топологию на Processor API: источники, процессоры, хранилища и выходной топик. Топологию используют и приложение, и тесты. |
| `BlockedUsersProcessor` | По событиям из `blocked_users` обновляет список заблокированных в `blocked-users-store`. |
| `BlockFilterProcessor` | Отбрасывает сообщение, если отправитель есть в списке блокировок получателя. Остальные передаёт в `CensorProcessor`. |
| `BannedWordsProcessor` | По записям из `banned_words` обновляет глобальное хранилище `banned-words-store`: запись добавляет слово, tombstone удаляет. |
| `CensorProcessor` | Заменяет запрещённые слова звёздочками и отправляет сообщение в `filtered_messages`. |
| `Message` | Сообщение: `from` (отправитель), `to` (получатель), `text`. |
| `BlockEvent` | Событие блокировки: `blockedUser` (кого блокируют) и `action` (`BLOCK` или `UNBLOCK`). Кто блокирует, указано в ключе записи. |
| `JsonSerde` | Serde для значений в JSON на Jackson 3. Тип передаётся через `TypeReference`, поэтому работают и обобщённые типы, например `Set<String>` в хранилище блокировок. |
| `KafkaUtils` | Настройки Kafka Streams, Admin-клиент и продюсер. Адрес брокера, `application.id` и каталог состояния берёт из `application.properties`. |
| `TestDataProducer` | Отправляет тестовый сценарий в 4 шага с паузами по 3 секунды (см. «Тестовые данные»). |

Тесты:

| Класс | Назначение |
|---|---|
| `MessageTopologyTest` | 9 тестов топологии на `TopologyTestDriver`, без брокера. |
| `KafkaUnit2ApplicationTests` | Проверяет, что контекст Spring поднимается. `StreamsRunner` в нём заменён заглушкой, поэтому Kafka не нужна. |

## Настройки

Файл `src/main/resources/application.properties`:

| Свойство | Значение | Переменная в `docker-compose.yml` | Назначение |
|---|---|---|---|
| `kafka.bootstrap-servers` | `localhost:9092` | `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` | адрес брокера |
| `kafka.application-id` | `message-filter-app` | — | `application.id` Kafka Streams: группа консьюмеров и префикс служебных топиков |
| `kafka.state-dir` | `build/kafka-streams-state` | `KAFKA_STATE_DIR=/app/state` | каталог для файлов хранилищ (RocksDB) |
| `app.test-data.enabled` | `true` | `APP_TEST_DATA_ENABLED`, по умолчанию `true` | отправлять ли тестовые данные при старте |

Любое свойство можно переопределить переменной окружения. Если сменить `kafka.application-id`, Kafka Streams будет считать приложение новым. У него появятся новая группа консьюмеров и новый changelog, и все топики будут обработаны заново с самого начала.

У брокера два адреса: контейнеры в сети Docker подключаются к `kafka:29092`, приложения на хосте — к `localhost:9092`.

## Запуск

Нужны Docker Desktop и JDK 21. Команды ниже написаны для Git Bash. В PowerShell вместо `./gradlew` используйте `gradlew.bat`.

Стек из `../kafka-unit-1` должен быть остановлен. В обоих `docker-compose.yml` есть контейнер с именем `zookeeper`, и оба стека занимают порты 2181, 8080, 8081 и 8088.

### 1. Собрать jar

```bash
./gradlew bootJar
```

Образ приложения не собирает проект, а копирует готовый `build/libs/kafka-unit-2-0.0.1-SNAPSHOT.jar`. Поэтому jar нужно пересобирать после каждого изменения кода.

### 2. Запустить Kafka и приложение

```bash
docker compose up -d --build
docker compose logs -f app
```

При первом запуске Docker скачивает образы. Приложение ждёт, пока брокер будет готов, и примерно через 30–40 секунд пишет в лог `Созданы топики: [messages, filtered_messages, blocked_users, banned_words]`. Если брокер так и не ответил, приложение завершается с ошибкой, и Docker перезапускает его (`restart: on-failure`).

Сразу после старта приложение отправляет тестовые данные (см. «Тестирование», шаг 2). Чтобы запустить его без них:

```bash
APP_TEST_DATA_ENABLED=false docker compose up -d --build
```

Kafka UI доступен по адресу http://localhost:8080. Там можно посмотреть топики и сообщения в них, а также отправить сообщение вручную.

### Запуск с хоста

Приложение можно запустить и с хоста, из IDE или командой ниже. Kafka при этом остаётся в Docker. Контейнер `app` нужно остановить, иначе два экземпляра с одинаковым `application.id` поделят партиции между собой.

```bash
docker compose stop app
./gradlew bootJar
java -Dstdout.encoding=UTF-8 -jar build/libs/kafka-unit-2-0.0.1-SNAPSHOT.jar
```

Флаг `-Dstdout.encoding=UTF-8` нужен, чтобы русский текст в консоли Windows не превращался в `????`. Состояние хранилищ хранится в `build/kafka-streams-state`. Предупреждение `Failed to change permissions for the directory` в Windows безвредно: Kafka Streams пытается выставить права доступа POSIX, а Windows их не поддерживает.

### 3. Остановить

```bash
docker compose stop app     # только приложение
docker compose stop         # всё
docker compose down -v      # удалить контейнеры и volume app-state
```

`stop_grace_period: 15s` в `docker-compose.yml` даёт приложению время закрыть Kafka Streams.

Данные брокера и ZooKeeper лежат в анонимных volume, их объявляют образы cp-kafka и cp-zookeeper. После `docker compose down` новые контейнеры получают новые пустые volume, так что топики пропадают в любом случае. Поэтому для сброса используйте `docker compose down -v`. Без `-v` старые анонимные volume остаются на диске, а в volume `app-state` остаётся локальное состояние хранилищ от удалённых топиков.

## Тестирование

### 1. Автотесты (без брокера)

```bash
./gradlew test
```

`MessageTopologyTest` проверяет топологию через `TopologyTestDriver`. Он обрабатывает каждую запись синхронно, сразу при отправке, без брокера и без пауз.

| Тест | Что проверяет |
|---|---|
| `messageWithoutBlocksOrBannedWordsIsDeliveredUnchanged` | сообщение без блокировок и запрещённых слов доходит без изменений |
| `messageFromBlockedUserIsDropped` | сообщение от заблокированного отбрасывается, от остальных — доходит |
| `blockAffectsOnlyRecipientWhoBlocked` | блокировка действует только у того, кто заблокировал |
| `unblockedUserCanSendAgain` | после `UNBLOCK` сообщения снова доходят |
| `eachUserHasOwnBlockListInStore` | у каждого пользователя свой список в `blocked-users-store`, пустой список удаляется |
| `bannedWordsAreMaskedIgnoringCase` | слова маскируются без учёта регистра и только целиком |
| `bannedWordsCanBeAddedAndRemovedOnTheFly` | слово можно добавить и удалить (tombstone) на лету |
| `blockedMessageIsNotCensoredAndDeliveredOneIs` | цензура применяется к доставленным сообщениям |
| `invalidJsonIsSkipped` | запись с некорректным JSON пропускается, обработка продолжается |

Запустить один тест:

```bash
./gradlew test --tests "com.dmitrykoryanov.kafka.kafkaunit2.MessageTopologyTest.messageFromBlockedUserIsDropped"
```

### 2. Автоматические тестовые данные

Запустите стенд (раздел «Запуск», шаги 1–2). При старте `TestDataProducer` отправляет сценарий из раздела «Тестовые данные»: 4 шага с паузами по 3 секунды. Записи из разных топиков Kafka Streams обрабатывает независимо друг от друга. Паузы нужны, чтобы блокировки и изменения списка слов применились раньше, чем придут следующие сообщения.

**Результат в топике `filtered_messages`:**

```bash
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic filtered_messages \
  --from-beginning --property print.key=true
```

Ожидаемый результат — 5 сообщений из 8. Сообщения из разных партиций могут прийти в другом порядке:

```
Иван	{"from":"Мария","to":"Иван","text":"Привет, Иван! Как дела?"}
Анна	{"from":"Михаил","to":"Анна","text":"Ты *****, купи наш ****"}
Анна	{"from":"Мария","to":"Анна","text":"Анна, извини!"}
Анна	{"from":"Михаил","to":"Анна","text":"Огромная ****** на spam"}
Мария	{"from":"Иван","to":"Мария","text":"Мария, ты не *****"}
```

Консьюмер не завершается сам, остановите его по Ctrl+C. Тестовые данные отправляются при каждом старте приложения, поэтому после перезапусков сообщения в топике повторяются.

**Логи процессоров:**

```bash
docker compose logs app | grep -E "BlockedUsersProcessor|BlockFilterProcessor|BannedWordsProcessor|CensorProcessor"
```

Ожидаемый результат (из строк убраны дата и имя потока):

```
BannedWordsProcessor    : Слово «дурак» добавлено в список запрещённых
BannedWordsProcessor    : Слово «spam» добавлено в список запрещённых
BannedWordsProcessor    : Слово «идиот» добавлено в список запрещённых
BlockedUsersProcessor   : Пользователь Иван: блокировка пользователя Михаил, список заблокированных: [Михаил]
BlockedUsersProcessor   : Пользователь Анна: блокировка пользователя Мария, список заблокированных: [Мария]
CensorProcessor         : Сообщение Иван -> Мария отцензурировано: «Мария, ты не идиот» -> «Мария, ты не *****»
BlockFilterProcessor    : Сообщение Михаил -> Иван не доставлено: отправитель в списке блокировок получателя
CensorProcessor         : Сообщение Михаил -> Анна отцензурировано: «Ты дурак, купи наш SPAM» -> «Ты *****, купи наш ****»
BlockFilterProcessor    : Сообщение Мария -> Анна не доставлено: отправитель в списке блокировок получателя
BannedWordsProcessor    : Слово «скидка» добавлено в список запрещённых
BannedWordsProcessor    : Слово «spam» удалено из списка запрещённых
BlockedUsersProcessor   : Пользователь Анна: снятие блокировки пользователя Мария, список заблокированных: []
CensorProcessor         : Сообщение Михаил -> Анна отцензурировано: «Огромная скидка на spam» -> «Огромная ****** на spam»
BlockFilterProcessor    : Сообщение Михаил -> Иван не доставлено: отправитель в списке блокировок получателя
```

### 3. Ручная отправка тестовых данных

Запустите стенд с нуля и без автоматических тестовых данных:

```bash
docker compose down -v
APP_TEST_DATA_ENABLED=false docker compose up -d --build
docker compose logs -f app
```

Дождитесь в логе строки `Созданы топики: [...]`, а затем строки `State transition from REBALANCING to RUNNING`. Только после этого отправляйте данные. Если отправить запись в топик, которого ещё нет, брокер создаст его сам с одной партицией. Тогда приложение остановится с ошибкой `У топиков messages и blocked_users разное число партиций`, и стенд придётся пересоздать через `docker compose down -v`.

Отправьте данные из раздела «Тестовые данные» шаг за шагом и проверьте результат так же, как в шаге 2. Сценарий можно повторять: шаг 1 заново задаёт блокировки и слова, поэтому повторная отправка даёт тот же результат.

### 4. Дополнительные проверки

**Топики созданы программно.**

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe \
  --topic 'messages|filtered_messages|blocked_users|banned_words' | grep PartitionCount
```

```
Topic: blocked_users	TopicId: ...	PartitionCount: 3	ReplicationFactor: 1	Configs:
Topic: filtered_messages	TopicId: ...	PartitionCount: 3	ReplicationFactor: 1	Configs:
Topic: messages	TopicId: ...	PartitionCount: 3	ReplicationFactor: 1	Configs:
Topic: banned_words	TopicId: ...	PartitionCount: 1	ReplicationFactor: 1	Configs: cleanup.policy=compact
```

**Списки блокировок лежат в persistent state store.** Содержимое хранилища видно в его changelog-топике. Каждая запись — новое состояние списка пользователя, `null` означает, что список удалён:

```bash
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic message-filter-app-blocked-users-store-changelog --from-beginning --property print.key=true
```

```
Иван	["Михаил"]
Анна	["Мария"]
Анна	null
```

Файлы RocksDB лежат в volume `app-state`: по каталогу на каждую задачу (`0_0`, `0_1`, `0_2`) и отдельный каталог для глобального хранилища. Без `MSYS_NO_PATHCONV=1` Git Bash превратит путь `/app/...` в путь Windows:

```bash
MSYS_NO_PATHCONV=1 docker exec kafka-unit-2-app-1 ls /app/state/message-filter-app
```

```
0_0
0_1
0_2
global
kafka-streams-process-metadata
```

**Блокировки переживают перезапуск.** Пересоздайте контейнер приложения без тестовых данных, чтобы блокировки не пришли заново. Volume `app-state` при этом сохраняется.

```bash
APP_TEST_DATA_ENABLED=false docker compose up -d --force-recreate app
docker compose logs -f app      # дождаться «State transition from REBALANCING to RUNNING»
```

Затем отправьте два сообщения:

```bash
docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 --topic messages \
  --property parse.key=true --property key.separator='|' <<'EOF'
Иван|{"from":"Михаил","to":"Иван","text":"Иван, ты тут?"}
Анна|{"from":"Мария","to":"Анна","text":"Анна, ты не дурак"}
EOF
```

Ожидаемый результат в логе: блокировка Михаила у Ивана сохранилась, список слов тоже.

```
BlockFilterProcessor    : Сообщение Михаил -> Иван не доставлено: отправитель в списке блокировок получателя
CensorProcessor         : Сообщение Мария -> Анна отцензурировано: «Анна, ты не дурак» -> «Анна, ты не *****»
```

**Некорректный JSON пропускается.**

```bash
echo 'Иван|not a json' | docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 \
  --topic messages --property parse.key=true --property key.separator='|'
docker compose logs --since 1m app | grep -E "Deserialization|Skipping record|Unrecognized token"
```

Ожидаемый результат (номера партиции и offset могут отличаться):

```
WARN ... LogAndContinueExceptionHandler : Exception caught during Deserialization, taskId: 0_1, topic: messages, partition: 1, offset: 22
tools.jackson.core.exc.StreamReadException: Unrecognized token 'not': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')
WARN ... RecordDeserializer             : ... task [0_1] Skipping record due to deserialization error. topic=[messages] partition=[1] offset=[22]
tools.jackson.core.exc.StreamReadException: Unrecognized token 'not': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')
```

После этого приложение продолжает работать: `docker compose ps app` показывает контейнер в состоянии `Up`, и следующие сообщения обрабатываются.

## Тестовые данные

Сценарий проверяет обе подзадачи: блокировку и разблокировку, цензуру, добавление и удаление запрещённого слова на лету. Его же отправляет `TestDataProducer`, а `MessageTopologyTest` проверяет те же случаи.

| Шаг | Топик | Ключ | Значение | Ожидаемый результат |
|---|---|---|---|---|
| 1 | `banned_words` | `дурак` | `дурак` | слово запрещено |
| 1 | `banned_words` | `spam` | `spam` | слово запрещено |
| 1 | `banned_words` | `идиот` | `идиот` | слово запрещено |
| 1 | `blocked_users` | `Иван` | `{"blockedUser":"Михаил","action":"BLOCK"}` | Иван блокирует Михаила |
| 1 | `blocked_users` | `Анна` | `{"blockedUser":"Мария","action":"BLOCK"}` | Анна блокирует Марию |
| 2 | `messages` | `Иван` | `{"from":"Мария","to":"Иван","text":"Привет, Иван! Как дела?"}` | доходит без изменений |
| 2 | `messages` | `Иван` | `{"from":"Михаил","to":"Иван","text":"Купи наш spam!"}` | не доходит: Михаил в списке Ивана |
| 2 | `messages` | `Анна` | `{"from":"Михаил","to":"Анна","text":"Ты дурак, купи наш SPAM"}` | `Ты *****, купи наш ****` |
| 2 | `messages` | `Анна` | `{"from":"Мария","to":"Анна","text":"Привет, Анна!"}` | не доходит: Мария в списке Анны |
| 2 | `messages` | `Мария` | `{"from":"Иван","to":"Мария","text":"Мария, ты не идиот"}` | `Мария, ты не *****` |
| 3 | `banned_words` | `скидка` | `скидка` | слово запрещено |
| 3 | `banned_words` | `spam` | `null` (tombstone) | слово больше не запрещено |
| 3 | `blocked_users` | `Анна` | `{"blockedUser":"Мария","action":"UNBLOCK"}` | Анна разблокирует Марию |
| 4 | `messages` | `Анна` | `{"from":"Мария","to":"Анна","text":"Анна, извини!"}` | теперь доходит |
| 4 | `messages` | `Анна` | `{"from":"Михаил","to":"Анна","text":"Огромная скидка на spam"}` | `Огромная ****** на spam` |
| 4 | `messages` | `Иван` | `{"from":"Михаил","to":"Иван","text":"Иван, ответь!"}` | по-прежнему не доходит |

Ключ сообщения — получатель, ключ события блокировки — тот, кто блокирует. С другими ключами список блокировок и сообщения окажутся в разных партициях, и фильтрация не сработает.

### Отправка через консольный продюсер

Команды ниже отправляют данные из таблицы через `kafka-console-producer` внутри контейнера брокера. Каждая строка — одна запись в формате `ключ|значение`. Разделитель `|` выбран потому, что в JSON есть двоеточия. Шаги отправляйте по очереди, с паузой в пару секунд между ними.

Команды написаны для Git Bash. В Windows PowerShell 5.1 они не работают. Там нет heredoc (`<<'EOF'`), а при передаче текста в `docker` через конвейер возникает одна из двух проблем, в зависимости от `$OutputEncoding`: кириллица превращается в `?` или в начало ключа первой записи попадает невидимый символ BOM. Записи можно отправить и через Kafka UI: Topics → топик → Produce Message, ключ и значение вводятся в отдельные поля.

**Шаг 1. Запрещённые слова и блокировки.**

```bash
docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 --topic banned_words \
  --property parse.key=true --property key.separator='|' <<'EOF'
дурак|дурак
spam|spam
идиот|идиот
EOF

docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 --topic blocked_users \
  --property parse.key=true --property key.separator='|' <<'EOF'
Иван|{"blockedUser":"Михаил","action":"BLOCK"}
Анна|{"blockedUser":"Мария","action":"BLOCK"}
EOF
```

**Шаг 2. Сообщения.**

```bash
docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 --topic messages \
  --property parse.key=true --property key.separator='|' <<'EOF'
Иван|{"from":"Мария","to":"Иван","text":"Привет, Иван! Как дела?"}
Иван|{"from":"Михаил","to":"Иван","text":"Купи наш spam!"}
Анна|{"from":"Михаил","to":"Анна","text":"Ты дурак, купи наш SPAM"}
Анна|{"from":"Мария","to":"Анна","text":"Привет, Анна!"}
Мария|{"from":"Иван","to":"Мария","text":"Мария, ты не идиот"}
EOF
```

**Шаг 3. Изменение списка слов и разблокировка.** Консольный продюсер из `cp-kafka:7.0.1` не умеет отправлять запись со значением `null`. Опция `null.marker` появилась в более новых версиях Kafka. Поэтому tombstone для слова `spam` отправляется продюсером из образа `confluentinc/cp-kafka:7.5.0`, который запускается в сети стенда. Строка `NULL` в нём означает пустое значение. Сеть называется `<каталог проекта>_kafka-network`. Если каталог проекта называется не `kafka-unit-2`, найдите её имя командой `docker network ls`.

```bash
docker run --rm -i --network kafka-unit-2_kafka-network confluentinc/cp-kafka:7.5.0 \
  kafka-console-producer --bootstrap-server kafka:29092 --topic banned_words \
  --property parse.key=true --property key.separator='|' --property null.marker=NULL <<'EOF'
скидка|скидка
spam|NULL
EOF

docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 --topic blocked_users \
  --property parse.key=true --property key.separator='|' <<'EOF'
Анна|{"blockedUser":"Мария","action":"UNBLOCK"}
EOF
```

**Шаг 4. Сообщения после изменений.**

```bash
docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 --topic messages \
  --property parse.key=true --property key.separator='|' <<'EOF'
Анна|{"from":"Мария","to":"Анна","text":"Анна, извини!"}
Анна|{"from":"Михаил","to":"Анна","text":"Огромная скидка на spam"}
Иван|{"from":"Михаил","to":"Иван","text":"Иван, ответь!"}
EOF
```

Результат в `filtered_messages` и строки в логе должны совпасть с ожидаемыми из шага 2 раздела «Тестирование».
