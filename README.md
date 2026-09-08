# SynXpo

Проект находится в стадии переосмысления серверной архитектуры.

Старая реализация клиента, сервера и протокола синхронизации удалена. Новая
версия будет строиться вокруг долговечных upload-сессий, staging-хранилища и
атомарной публикации ревизий. Текущее архитектурное решение описано в
[`adr-server.md`](adr-server.md).

Описание состава сервиса, границ слоёв и целевого data flow находится в
[`docs/architecture.md`](docs/architecture.md).

Доменные типы расположены в `libs/domain`, контракт persistence — в `libs/db`.
Production persistence будет реализован PostgreSQL-адаптером на userver uPg;
миграции живут в `migrations/postgres/` и фиксируются в `schema_migrations`.

## Сборка

Быстрые domain-тесты не требуют PostgreSQL:

```bash
cmake -S . -B build -DSYNXPO_BUILD_TESTS=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

Для server-конфигурации CMake скачает userver v3.1 через FetchContent:

```bash
cmake -S . -B build-server -DSYNXPO_BUILD_SERVER=ON
cmake --build build-server --target synxpo-server --parallel
```

Перед запуском создайте `config_vars.yaml` по примеру
`apps/server/config/config_vars.example.yaml`, укажите DSN PostgreSQL и
примените миграции из `migrations/postgres/`. Автоматический migration runner
будет добавлен вместе с `db_postgres`.
