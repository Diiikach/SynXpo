# Архитектура SynXpo

SynXpo — сервер синхронизации папок. Сервер является источником истины для
ревизий директории: клиент сначала загружает изменения во временную область,
а затем публикует их одной операцией commit.

## Слои

```text
HTTP handlers / gRPC service       apps/server, transport_*
              │
Application use cases             libs/upload_sessions (planned)
              │
Repository contract               libs/db
              │
PostgreSQL adapter (uPg)          libs/db_postgres (planned)
              │
PostgreSQL

Domain rules                      libs/domain
Staging file adapter              libs/staging_fs
```

`domain` содержит только типы и правила: состояния upload-сессии, manifest и
разрешённые переходы. Он не зависит от транспорта, userver, PostgreSQL или
файловой системы.

`db` определяет `UploadSessionRepository`. Application use cases обращаются
только к нему. `db_postgres` будет инфраструктурной реализацией контракта на
асинхронном uPg driver userver.

userver разрешён только на инфраструктурной границе: `apps/server`, transport
libraries и `db_postgres`.

## Сервис и конфигурация

Точка входа — `apps/server/main.cpp`. Она регистрирует:

- HTTP listener на порту `http_port` (по умолчанию `8080`);
- gRPC listener на порту `grpc_port` (по умолчанию `8091`);
- PostgreSQL component `upload-database`;
- task processors `main-task-processor` и `fs-task-processor`.

Static config хранится в `apps/server/config/static_config.yaml`. Секреты и
переменные окружения в неё не помещаются. Их передают через файл config vars:

```bash
cp apps/server/config/config_vars.example.yaml config_vars.yaml
./build-server/apps/server/synxpo-server \
  -c apps/server/config/static_config.yaml \
  --config-vars config_vars.yaml
```

`postgres_dsn` должен указывать на доступный PostgreSQL. Пример рассчитан на
локальную базу `synxpo`; пользователь и пароль нужно заменить перед запуском.

## Данные

`migrations/postgres/0001_initial.sql` создаёт:

- `directories` — директории и их опубликованная ревизия;
- `upload_sessions` — долговечные операции загрузки;
- `upload_session_files` — неизменяемый manifest файлов сессии;
- `schema_migrations` — применённые версии схемы.

Миграции immutable: исправление схемы всегда оформляется следующим файлом
`NNNN_description.sql`. Пока migration runner отсутствует, миграции применяют
вручную; после появления `db_postgres` это станет частью запуска сервиса.

## Целевой upload flow

1. Клиент создаёт upload-сессию HTTP-запросом и передаёт manifest.
2. Сервер фиксирует сессию и файлы в PostgreSQL со статусом `created`.
3. Клиент отправляет gRPC stream чанков одного файла строго по порядку.
4. Байты попадают в `staging/<session-id>/<file-id>`, а received offset
   сохраняется в PostgreSQL. После разрыва клиент продолжает с этого offset.
5. После получения всех файлов сервер сверяет размер и content hash.
6. Commit проверяет base revision, переносит staging-данные в опубликованное
   хранилище и атомарно создаёт следующую ревизию.
7. Другие устройства получают revision и содержимое через download API.

Пункты 1–7 пока являются целевой реализацией. Сейчас готовы domain rules,
repository contract, начальная PostgreSQL schema, staging skeleton и server
composition/configuration.

## Тестирование

- Unit: правила состояний в `libs/domain/tests`.
- Component: будущие use cases с in-memory fake repository.
- Integration: `db_postgres` с реальным PostgreSQL и миграциями.
- API: HTTP/gRPC handlers с тестовым server и PostgreSQL.
- E2E: создание сессии, upload, resume, commit и download между клиентами.
