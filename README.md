# Kafka + ClickHouse: домашнее задание

Проект загружает CSV-датасет `train.csv` в **Kafka**, обрабатывает его через **ClickHouse** и выполняет аналитический SQL-запрос: **категория наибольшей транзакции по каждому штату**.

---

## Архитектура

```text
train.csv
   │
   ▼
Python producer (producer/kafka_producer.py)
   │
   ▼
Kafka topic: transactions
   │
   ▼
ClickHouse: Kafka Engine → Materialized View → MergeTree
   │
   ▼
SQL-запрос: argMax(cat_id, amount) GROUP BY us_state
   │
   ▼
results/max_tx_category_by_state.csv
```

## Структура проекта

```text
kafka-clickhouse-hw/
├── docker-compose.yml
├── requirements.txt
├── README.md
├── producer/
│   └── kafka_producer.py
├── sql/
│   ├── 01_ddl_base.sql
│   ├── 02_ddl_optimized.sql
│   └── 03_max_tx_category_by_state.sql
├── results/
│   ├── max_tx_category_by_state.csv
│   ├── max_tx_category_by_state_base.csv
│   └── max_tx_category_by_state_optimized.csv
└── data/
    └── train.csv
```

## Требования

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Python 3.10+
- Git
- Датасет `train.csv` — [Kaggle](https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394)

---

## Как запустить

### 1. Скачать датасет

Принять правила соревнования на Kaggle, скачать `train.csv` и положить в `data/train.csv`.

### 2. Запустить контейнеры

```bash
docker compose up -d
```

Подождать около 40 секунд и убедиться, что оба контейнера (`kafka`, `clickhouse`) в статусе `running`:

```bash
docker compose ps
```

### 3. Создать таблицы в ClickHouse

Базовая схема:

```bash
docker exec -i clickhouse clickhouse-client --multiquery < sql/01_ddl_base.sql
```

Оптимизированная схема:

```bash
docker exec -i clickhouse clickhouse-client --multiquery < sql/02_ddl_optimized.sql
```

### 4. Установить Python-зависимости

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

> **Важно:** на Python 3.12+ библиотека `kafka-python` не работает, поэтому используется форк `kafka-python-ng`.

### 5. Загрузить CSV в Kafka

```bash
python producer/kafka_producer.py
```

Скрипт читает CSV и отправляет каждую строку в топик `transactions`. Дождитесь сообщения `Producer finished`.

### 6. Проверить загрузку в ClickHouse

```bash
docker exec -i clickhouse clickhouse-client --query="SELECT count() FROM teta.transactions"
```

Ожидаемый результат — около **786 431** строк (равно числу строк в `train.csv`).

### 7. Выполнить аналитический запрос

```bash
docker exec -i clickhouse clickhouse-client --query="
SELECT
    us_state,
    argMax(cat_id, amount) AS top_category,
    max(amount)            AS max_amount
FROM teta.transactions
GROUP BY us_state
ORDER BY max_amount DESC
FORMAT CSVWithNames" > results/max_tx_category_by_state.csv
```

Результат — CSV следующего вида:

```csv
"us_state","top_category","max_amount"
"NY","travel",27390.12
"SC","travel",26544.12
...
```

---

## Оптимизация

Оптимизированная схема (`sql/02_ddl_optimized.sql`) отличается от базовой:

| Приём | Зачем |
|-------|-------|
| `LowCardinality(String)` для `us_state`, `gender`, `jobs` | Словарное кодирование: меньше места на диске и быстрее группировка |
| `PARTITION BY toYYYYMM(transaction_time)` | Партиционирование по месяцам: быстрое отсечение партиций при фильтрации по дате |
| `ORDER BY (us_state, transaction_time)` | Сначала низкокардинальное поле, затем время — оптимальный порядок сортировки |
| `index_granularity = 8192` | Баланс между размером индекса и скоростью поиска |

### Замеры времени

Запрос «категория наибольшей транзакции по штату», датасет — 786 431 строка:

| Схема | Время выполнения |
|-------|------------------|
| Базовая | 0.271 s |
| Оптимизированная | 0.257 s |
