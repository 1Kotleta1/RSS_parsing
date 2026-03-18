[README.md](https://github.com/user-attachments/files/26091398/README.md)
# Multi-source news parser

Отдельный проект (вне `пример`) для параллельного чтения новостей из:
- `https://www.rbc.ru/short_news/`
- `https://ria.ru/` (только ссылки с главной страницы)
- `https://dzen.ru/news`
- `https://lenta.ru/`
- `https://tproger.ru/news`
- `https://ren.tv/news`
- `https://www.mk.ru/news/`
- `https://www.m24.ru/news`
- `https://www.gazeta.ru/news/`

С подробными логами:
- в консоль,
- в файл `logs/latest.log`.

По умолчанию парсер запускает все источники одновременно, ждет результаты и
объединяет их в один JSON.

## Windows

```cmd
run_windows.cmd
```

## Linux/macOS

```bash
bash run_linux.sh
```

Результат сохраняется в:

`news/news_YYYY-MM-DD.json`

## Docker (рекомендуется для контроля логов)

### One-shot парсинг (старый режим)

Сборка и запуск:

```bash
docker compose --profile batch up --build
```

Windows one-click:

```cmd
run_docker.cmd
```

Где смотреть:
- JSON: `news/news_YYYY-MM-DD.json`
- Логи: `logs/latest.log`

Повторный запуск без ребилда:

```bash
docker compose --profile batch up
```

Поток логов контейнера:

```bash
docker compose --profile batch logs -f
```

### Локальный API для n8n

Этот режим поднимает API на:
- `http://localhost:8080`

Запуск (постоянно в фоне):

```bash
docker compose --profile api up -d --build
```

Windows one-click:

```cmd
run_docker_api.cmd
```

Остановка API:

```bash
docker compose --profile api stop
```

Windows:

```cmd
stop_docker_api.cmd
```

Проверка:
- локально: `GET http://localhost:8080/health`

Запуск парсера через API (для n8n):

```bash
curl -X POST "http://localhost:8080/run" \
  -H "Content-Type: application/json" \
  -d "{\"hours\":24,\"max_search_seconds\":300,\"include_items\":true}"
```

Ответ API содержит:
- `result.items` (новости)
- `result.stats` (детальная статистика)
- `output_file` (путь к JSON в контейнере)

Если n8n запущен на другой машине в вашей сети, используйте IP хоста с Docker:
- `http://<HOST_LOCAL_IP>:8080/run`

### Туннель для внешнего пользователя (localhost.run)

Если нужно, чтобы другой пользователь делал POST/GET снаружи, поднимите туннель:

1) Запустите API:

```bash
docker compose --profile api up --build
```

2) В другом окне терминала запустите туннель:

Windows:

```cmd
run_tunnel_localhostrun.cmd
```

Linux/macOS:

```bash
bash run_tunnel_localhostrun.sh
```

3) Скопируйте публичный URL из вывода (`https://xxxxx.localhost.run`) и используйте:
- `GET https://xxxxx.localhost.run/health`
- `POST https://xxxxx.localhost.run/run`

Пример POST:

```bash
curl -X POST "https://xxxxx.localhost.run/run" \
  -H "Content-Type: application/json" \
  -d "{\"hours\":24,\"max_search_seconds\":300,\"include_items\":true}"
```
