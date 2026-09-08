# SynXpo

Проект находится в стадии переосмысления серверной архитектуры.

Старая реализация клиента, сервера и протокола синхронизации удалена. Новая
версия будет строиться вокруг долговечных upload-сессий, staging-хранилища и
атомарной публикации ревизий. Текущее архитектурное решение описано в
[`adr-server.md`](adr-server.md).

Доменные типы расположены в `libs/domain`, контракт persistence — в `libs/db`,
а текущая реализация — в SQLite-адаптере `libs/db_sqlite`. SQL-миграции живут
в `migrations/sqlite/` и применяются при инициализации репозитория; версии
фиксируются в таблице `schema_migrations`.

## Сборка
```bash
cmake -S . -B build -DSYNXPO_BUILD_TESTS=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```
