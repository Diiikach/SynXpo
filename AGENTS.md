# SynXpo: текущий контекст разработки

ПРИ ИЗМЕНЕНИИ КОДОВОЙ БАЗЫ ПЕРЕД КАЖДЫМ COMMIT АКТУАЛИЗИРОВАТЬ ЭТОТ ФАЙЛИК.

## Состояние проекта

- Активная ветка: `feature/server-v2`.
- Старая реализация клиента, сервера, gRPC-протокола и синхронизации удалена.
- Проект находится на первом этапе новой серверной архитектуры. Решения и
  целевая модель описаны в [`adr-server.md`](adr-server.md).
- Уже реализован фундамент persistence для upload-сессий: SQLite-схема,
  `upload_sessions`, `upload_session_files`, состояния сессии, CAS-переходы,
  очистка истёкших сессий и versioned SQL-миграции.
- HTTP, gRPC, приём файлов в staging, проверка хешей, commit ревизий и
  исполняемый файл `synxpo-server` пока не реализованы.

## Текущая структура

```text
libs/domain/            # доменные типы и правила переходов
libs/db/                # абстрактный контракт UploadSessionRepository
libs/db_sqlite/         # SQLite adapter и его тесты
migrations/sqlite/      # нумерованные SQL-миграции SQLite
apps/server/            # будущий composition root / main.cpp, без бизнес-логики
```

Бизнес-логика не должна зависеть от HTTP, gRPC, SQLite или файловой системы.
Transport и конкретные реализации хранилища должны быть внешними адаптерами.

## Правила размещения кода

- У каждого модуля должна быть своя папка `tests/` рядом с его исходным кодом.
- Новые transport-слои размещать в отдельных библиотеках (`transport_http`,
  `transport_grpc`), а не в бизнес-логике.
- Application-код должен работать с `db::UploadSessionRepository`, а не с
  `db_sqlite::SqliteUploadSessionRepository`.
- Каждая новая SQLite-миграция — отдельный файл
  `migrations/sqlite/NNNN_description.sql`. Файлы нельзя изменять после
  поставки: исправления делаются следующей миграцией. Адаптер применяет их по
  номеру и отмечает в `schema_migrations` в той же транзакции.

## Сборка и проверка

```bash
cmake -S . -B build -DSYNXPO_BUILD_TESTS=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

SQLite3 — единственная текущая внешняя зависимость сборки.
