# SynXpo

## Установка

### Сборка из исходного кода
Склонируйте этот репозиторий и запустите следующие команды:
```bash
cmake -B build
cmake --build build
cmake --install build --prefix /path/to/installation # Путь к директории для установки
```
После этого вы сможете запускать клиент и сервер командами `synxpo-client` и `synxpo-server` соответственно. Вы также можете собрать только клиент или только сервер:
```bash
cmake -B build
cmake --build build --target synxpo-client # или synxpo-server
cmake --install build --prefix /path/to/installation --component client # или server
```
Если в вашей системе не установлены gRPC и Protocol Buffers, они будут загружены и собраны автоматически. Обратите внимание, что это довольно долгий процесс (в первый раз сборка может занимать 10-20 минут).
