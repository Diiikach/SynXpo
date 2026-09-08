# SynXpo: текущий контекст разработки

ПРИ ИЗМЕНЕНИИ КОДОВОЙ БАЗЫ ПЕРЕД КАЖДЫМ COMMIT АКТУАЛИЗИРОВАТЬ ЭТОТ ФАЙЛИК.

## Состояние проекта

- Активная ветка: `feature/server-v2`.
- Старая реализация клиента, сервера, gRPC-протокола и синхронизации удалена.
- Проект находится на первом этапе новой серверной архитектуры. Решения и
  целевая модель описаны в [`adr-server.md`](adr-server.md).
- SQLite persistence удалён. Единственный storage — PostgreSQL; начальная
  миграция находится в `migrations/postgres/`.
- `synxpo-server` уже имеет userver composition root и конфигурацию PostgreSQL
  pool `upload-database`, HTTP listener и gRPC listener.
- `db_postgres`, HTTP handlers, gRPC service, staging upload, проверка хешей и
  commit ревизий пока не реализованы.

## Текущая структура

```text
libs/domain/            # доменные типы и правила переходов
libs/db/                # абстрактный контракт UploadSessionRepository
libs/db_postgres/       # будущий PostgreSQL adapter на userver uPg
migrations/postgres/    # нумерованные SQL-миграции PostgreSQL
apps/server/            # composition root на userver, без бизнес-логики
docs/                   # архитектура, запуск и эксплуатационные заметки
```

Бизнес-логика не должна зависеть от HTTP, gRPC, PostgreSQL или файловой системы.
Transport и конкретные реализации хранилища должны быть внешними адаптерами.

## Правила размещения кода

- У каждого модуля должна быть своя папка `tests/` рядом с его исходным кодом.
- Новые transport-слои размещать в отдельных библиотеках (`transport_http`,
  `transport_grpc`), а не в бизнес-логике.
- Application-код должен работать с `db::UploadSessionRepository`, а не с
  конкретным PostgreSQL adapter.
- `userver` разрешён только на инфраструктурной границе: в `apps/server/`,
  `transport_http`, `transport_grpc` и `db_postgres`. Его типы, компоненты и
  coroutines не должны попадать в `domain`, `db` или application-слой.
- `db_postgres` реализует контракт через userver uPg; миграции PostgreSQL
  живут в `migrations/postgres/`.
- Каждая новая PostgreSQL-миграция — отдельный файл
  `migrations/postgres/NNNN_description.sql`. Файлы нельзя изменять после
  поставки: исправления делаются следующей миграцией. Адаптер применяет их по
  номеру и отмечает в `schema_migrations` в той же транзакции.

## Сборка и проверка

```bash
cmake -S . -B build -DSYNXPO_BUILD_TESTS=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

При `SYNXPO_BUILD_SERVER=ON` CMake FetchContent загружает userver v3.1 с gRPC
и PostgreSQL driver. Для сервера нужен доступный PostgreSQL и config vars;
пример находится в `apps/server/config/config_vars.example.yaml`.

## Ближайший план

1. Добавить `libs/upload_sessions` — application use cases поверх `db`.
2. Добавить `libs/staging_fs` для временных файлов и resume по offset.
3. Реализовать `db_postgres` и PostgreSQL integration-тесты.
4. Создать HTTP API сессий и gRPC streaming для данных файлов.
5. Реализовать commit ревизии и download path.
