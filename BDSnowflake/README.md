# BigData Snowflake — Лабораторная работа №1

Трансформация плоских данных о продажах товаров для домашних питомцев в аналитическую модель «снежинка» (Snowflake Schema) на PostgreSQL.

---

## Содержание

1. [Структура репозитория](#структура-репозитория)
2. [Описание исходных данных](#описание-исходных-данных)
3. [Схема снежинка — архитектура](#схема-снежинка--архитектура)
4. [Запуск](#запуск)
5. [Подключение и просмотр](#подключение-и-просмотр)
6. [Проверка схемы снежинка](#проверка-схемы-снежинка)
7. [Аналитические запросы](#аналитические-запросы)

---

## Структура репозитория

```
BDSnowflake/
├── исходные данные/
│   ├── MOCK_DATA.csv          # файл 0 (строки 1–1000)
│   ├── MOCK_DATA (1).csv      # файл 1 (строки 1–1000)
│   └── ... MOCK_DATA (9).csv  # итого 10 файлов × 1000 строк = 10 000
├── sql/
│   ├── 01_create_raw.sql      # DDL: staging-таблица mock_data
│   ├── 02_import_raw.sql      # импорт CSV → mock_data
│   ├── 03_ddl_snowflake.sql   # DDL: таблицы измерений и факта
│   └── 04_dml_snowflake.sql   # DML: заполнение схемы снежинка
├── docker-compose.yml
└── README.md
```

---

## Описание исходных данных

Каждый из 10 CSV-файлов содержит **1000 строк** с плоской (денормализованной) записью о продаже товара для питомца. Одна строка = одна продажа, в которой сразу содержится информация о клиенте, продавце, товаре, магазине и поставщике.

**Колонки исходной таблицы (49 полей):**

| Группа | Поля |
|--------|------|
| Клиент | `customer_first_name`, `customer_last_name`, `customer_age`, `customer_email`, `customer_country`, `customer_postal_code` |
| Питомец клиента | `customer_pet_type`, `customer_pet_name`, `customer_pet_breed`, `pet_category` |
| Продавец | `seller_first_name`, `seller_last_name`, `seller_email`, `seller_country`, `seller_postal_code` |
| Товар | `product_name`, `product_category`, `product_price`, `product_quantity`, `product_weight`, `product_color`, `product_size`, `product_brand`, `product_material`, `product_description`, `product_rating`, `product_reviews`, `product_release_date`, `product_expiry_date` |
| Продажа | `sale_date`, `sale_customer_id`, `sale_seller_id`, `sale_product_id`, `sale_quantity`, `sale_total_price` |
| Магазин | `store_name`, `store_location`, `store_city`, `store_state`, `store_country`, `store_phone`, `store_email` |
| Поставщик | `supplier_name`, `supplier_contact`, `supplier_email`, `supplier_phone`, `supplier_address`, `supplier_city`, `supplier_country` |

**Анализ уникальных сущностей (выявлено перед нормализацией):**

| Сущность | Уникальных | Ключ идентификации |
|----------|-----------|-------------------|
| Клиенты | 10 000 | `customer_email` |
| Продавцы | 10 000 | `seller_email` |
| Товары | ~10 000 | `product_name + brand + color + size + material + price` |
| Магазины | 9 998 | `store_name + store_city` |
| Поставщики | 9 993 | `supplier_name + supplier_city` |
| Категории товаров | 15 | `product_category + pet_category` |
| Страны | 230 | `country_name` |
| Даты продаж | 364 | `sale_date` |

---

## Схема снежинка — архитектура

**Снежинка** отличается от «звезды» тем, что измерения нормализованы — они сами имеют дочерние таблицы. В данной работе нормализация выполнена на двух уровнях:

```
                    ┌─────────────────┐
                    │   dim_country   │ ◄── общий справочник стран
                    └────────┬────────┘     (клиент, продавец,
                             │              магазин, поставщик)
          ┌──────────────────┼──────────────────┐
          │                  │                  │
   ┌──────┴───────┐  ┌───────┴──────┐  ┌────────┴───────┐  ┌──────────────┐
   │ dim_customer │  │  dim_seller  │  │   dim_store    │  │ dim_supplier │
   └──────┬───────┘  └──────────────┘  └────────────────┘  └──────────────┘
          │ (1:1)
   ┌──────┴───────┐      ┌──────────────────────┐
   │   dim_pet    │      │ dim_product_category │ ◄── тип товара + вид питомца
   └──────────────┘      └──────────┬───────────┘
                                    │
                          ┌─────────┴────────┐
                          │   dim_product    │
                          └─────────┬────────┘
                                    │
                    ┌───────────────▼────────────────────────────────┐
                    │                 fact_sales                      │
                    │  date_id │ customer_id │ seller_id │ product_id │
                    │  store_id │ supplier_id │ qty │ total_price     │
                    └────────────────────────────────────────────────┘
                                    │
                          ┌─────────┴────────┐
                          │    dim_date      │
                          └──────────────────┘
```

**Таблицы и их роли:**

| Таблица | Тип | Строк | Описание |
|---------|-----|-------|----------|
| `fact_sales` | Факт | 10 000 | Центральная таблица: числовые меры и FK на все измерения |
| `dim_date` | Измерение | 364 | Дата продажи с атрибутами: день, месяц, год, квартал, неделя |
| `dim_customer` | Измерение | 10 000 | Клиенты; FK → `dim_country` |
| `dim_pet` | Измерение (дочернее) | 10 000 | Питомцы клиентов; FK → `dim_customer` |
| `dim_seller` | Измерение | 10 000 | Продавцы; FK → `dim_country` |
| `dim_product` | Измерение | 10 000 | Товары; FK → `dim_product_category` |
| `dim_product_category` | Измерение (дочернее) | 15 | Тип товара × вид питомца; FK ← `dim_product` |
| `dim_store` | Измерение | 9 998 | Магазины; FK → `dim_country` |
| `dim_supplier` | Измерение | 9 993 | Поставщики; FK → `dim_country` |
| `dim_country` | Измерение (справочник) | 230 | Страны; используется 4 измерениями |
| `mock_data` | Staging | 10 000 | Исходные плоские данные (не удаляется — источник) |

**Признаки снежинки (не звезды):**
- `dim_pet` не подключена напрямую к `fact_sales` — только через `dim_customer`
- `dim_product_category` не подключена напрямую к `fact_sales` — только через `dim_product`
- `dim_country` — общий нормализованный справочник для 4 разных измерений

---

## Запуск

**Требования:** Docker (или OrbStack)

```bash
# 1. Клонировать репозиторий
git clone <url>
cd BDSnowflake

# 2. Запустить PostgreSQL и автоматически загрузить все данные
docker compose up -d

# 3. Дождаться готовности (20–30 секунд при первом запуске)
docker compose logs -f
# Ждать строку: database system is ready to accept connections
```

При первом запуске автоматически выполняются все 4 SQL-скрипта из папки `sql/`:
1. Создаётся staging-таблица `mock_data`
2. Импортируются 10 CSV-файлов (10 000 строк)
3. Создаётся схема снежинка (10 таблиц)
4. Таблицы заполняются из `mock_data`

**Остановить контейнер:**
```bash
docker compose down        # остановить, данные сохранятся
docker compose down -v     # остановить и удалить данные (полный сброс)
```

---

## Подключение и просмотр

### Через DBeaver (рекомендуется)

`Database` → `New Database Connection` → `PostgreSQL`

| Параметр | Значение |
|----------|----------|
| Host | `localhost` |
| Port | `5433` |
| Database | `petstore` |
| Username | `bigdata` |
| Password | `bigdata123` |

> **Важно:** порт `5433`, а не `5432` — чтобы не конфликтовать с локальным PostgreSQL.

После подключения в дереве слева: `petstore` → `Schemas` → `public` → `Tables` — видны все 11 таблиц.

### Через терминал (psql)

```bash
# С хоста
PGPASSWORD=bigdata123 psql -h localhost -p 5433 -U bigdata -d petstore

# Или внутри контейнера
docker exec -it bigdata_snowflake psql -h 127.0.0.1 -U bigdata -d petstore
```

### Полезные psql-команды

```sql
\dt                    -- список таблиц
\d fact_sales          -- структура таблицы с FK
\d dim_customer
```

---

## Проверка схемы снежинка

### 1. Все внешние ключи схемы

Запрос выводит граф связей — видно что это снежинка, а не звезда:

```sql
SELECT
    tc.table_name        AS от_таблицы,
    kcu.column_name      AS внешний_ключ,
    ccu.table_name       AS к_таблице,
    ccu.column_name      AS первичный_ключ
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.table_name;
```

**Ожидаемый результат:**

| от_таблицы | внешний_ключ | к_таблице | первичный_ключ |
|------------|-------------|-----------|---------------|
| dim_customer | country_id | dim_country | country_id |
| dim_pet | customer_id | dim_customer | customer_id |
| dim_product | category_id | dim_product_category | category_id |
| dim_seller | country_id | dim_country | country_id |
| dim_store | country_id | dim_country | country_id |
| dim_supplier | country_id | dim_country | country_id |
| fact_sales | customer_id | dim_customer | customer_id |
| fact_sales | date_id | dim_date | date_id |
| fact_sales | product_id | dim_product | product_id |
| fact_sales | seller_id | dim_seller | seller_id |
| fact_sales | store_id | dim_store | store_id |
| fact_sales | supplier_id | dim_supplier | supplier_id |

Строки `dim_pet → dim_customer` и `dim_product → dim_product_category` — **это и есть признак снежинки**: измерения ссылаются на другие измерения, а не напрямую на факт.

### 2. Количество строк во всех таблицах

```sql
SELECT 'mock_data'            AS таблица, COUNT(*) AS строк FROM mock_data
UNION ALL SELECT 'fact_sales',            COUNT(*) FROM fact_sales
UNION ALL SELECT 'dim_customer',          COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_pet',               COUNT(*) FROM dim_pet
UNION ALL SELECT 'dim_seller',            COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_product',           COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_product_category',  COUNT(*) FROM dim_product_category
UNION ALL SELECT 'dim_store',             COUNT(*) FROM dim_store
UNION ALL SELECT 'dim_supplier',          COUNT(*) FROM dim_supplier
UNION ALL SELECT 'dim_country',           COUNT(*) FROM dim_country
UNION ALL SELECT 'dim_date',              COUNT(*) FROM dim_date
ORDER BY строк DESC;
```

### 3. Проверка целостности — нет «висячих» FK

```sql
-- Все строки fact_sales имеют клиента в dim_customer
SELECT COUNT(*) AS без_клиента
FROM fact_sales fs
LEFT JOIN dim_customer c ON c.customer_id = fs.customer_id
WHERE c.customer_id IS NULL;
-- Результат: 0

-- Все строки fact_sales имеют дату в dim_date
SELECT COUNT(*) AS без_даты
FROM fact_sales fs
LEFT JOIN dim_date d ON d.date_id = fs.date_id
WHERE d.date_id IS NULL;
-- Результат: 0
```

### 4. Проверка иерархии снежинки — питомец через клиента

Путь `fact_sales → dim_customer → dim_pet` (два шага, не один):

```sql
SELECT
    fs.sale_id,
    c.first_name || ' ' || c.last_name AS клиент,
    p.pet_name                          AS питомец,
    p.pet_type                          AS вид,
    fs.sale_total_price                 AS сумма
FROM fact_sales fs
JOIN dim_customer c ON c.customer_id = fs.customer_id
JOIN dim_pet p      ON p.customer_id = c.customer_id  -- через dim_customer, не напрямую
LIMIT 10;
```

### 5. Проверка иерархии снежинки — категория через товар

Путь `fact_sales → dim_product → dim_product_category` (два шага):

```sql
SELECT
    fs.sale_id,
    pr.product_name                     AS товар,
    pc.category_name                    AS тип_товара,
    pc.pet_category                     AS для_питомца,
    fs.sale_total_price                 AS сумма
FROM fact_sales fs
JOIN dim_product pr          ON pr.product_id  = fs.product_id
JOIN dim_product_category pc ON pc.category_id = pr.category_id  -- через dim_product
LIMIT 10;
```

---

## Аналитические запросы

### Выручка по кварталам

```sql
SELECT
    d.year       AS год,
    d.quarter    AS квартал,
    COUNT(*)     AS кол_продаж,
    SUM(fs.sale_total_price)::NUMERIC(12,2) AS выручка
FROM fact_sales fs
JOIN dim_date d ON d.date_id = fs.date_id
GROUP BY d.year, d.quarter
ORDER BY 1, 2;
```

### Выручка по типу товара и виду питомца

```sql
SELECT
    pc.category_name                        AS тип_товара,
    pc.pet_category                         AS для_питомца,
    COUNT(*)                                AS кол_продаж,
    SUM(fs.sale_total_price)::NUMERIC(12,2) AS выручка,
    AVG(fs.sale_total_price)::NUMERIC(10,2) AS средний_чек
FROM fact_sales fs
JOIN dim_product p           ON p.product_id   = fs.product_id
JOIN dim_product_category pc ON pc.category_id = p.category_id
GROUP BY pc.category_name, pc.pet_category
ORDER BY выручка DESC;
```

### Топ стран по количеству клиентов

```sql
SELECT
    co.country_name  AS страна,
    COUNT(*)         AS клиентов
FROM dim_customer c
JOIN dim_country co ON co.country_id = c.country_id
GROUP BY co.country_name
ORDER BY клиентов DESC
LIMIT 10;
```

### Полный путь снежинки в одном запросе

Демонстрирует все уровни иерархии одновременно (6 джойнов):

```sql
SELECT
    d.year                                  AS год,
    d.month                                 AS месяц,
    co_c.country_name                       AS страна_клиента,
    p.pet_type                              AS вид_питомца,
    pc.category_name                        AS тип_товара,
    co_st.country_name                      AS страна_магазина,
    COUNT(*)                                AS продаж,
    SUM(fs.sale_total_price)::NUMERIC(12,2) AS выручка
FROM fact_sales fs
JOIN dim_date d              ON d.date_id     = fs.date_id
JOIN dim_customer c          ON c.customer_id = fs.customer_id
JOIN dim_country co_c        ON co_c.country_id = c.country_id
JOIN dim_pet p               ON p.customer_id = c.customer_id
JOIN dim_product pr          ON pr.product_id  = fs.product_id
JOIN dim_product_category pc ON pc.category_id = pr.category_id
JOIN dim_store st            ON st.store_id    = fs.store_id
JOIN dim_country co_st       ON co_st.country_id = st.country_id
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY выручка DESC
LIMIT 20;
```
