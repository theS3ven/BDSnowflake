-- Справочник стран (общий для клиентов, продавцов, магазинов и
-- поставщиков — ключевой элемент нормализации схемы снежинка)
CREATE TABLE dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL UNIQUE
);


-- Измерение: Клиенты
CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    age         INTEGER,
    email       VARCHAR(200) UNIQUE,
    postal_code VARCHAR(20),
    country_id  INTEGER REFERENCES dim_country(country_id)
);


-- Измерение: Питомцы (дочернее к dim_customer — нормализация снежинки)
-- Один клиент → один питомец (1:1 в данных)
CREATE TABLE dim_pet (
    pet_id       SERIAL PRIMARY KEY,
    customer_id  INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    pet_type     VARCHAR(50),
    pet_name     VARCHAR(100),
    pet_breed    VARCHAR(100),
    pet_category VARCHAR(100)
);


-- Измерение: Продавцы
CREATE TABLE dim_seller (
    seller_id   SERIAL PRIMARY KEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    email       VARCHAR(200) UNIQUE,
    postal_code VARCHAR(20),
    country_id  INTEGER REFERENCES dim_country(country_id)
);


-- Измерение: Категории товаров (дочернее к dim_product — нормализация снежинки)
-- category_name — тип товара (Food, Toy, Cage)
-- pet_category  — вид питомца (Dogs, Cats, Birds, Fish, Reptiles)
-- Уникальность по паре: каждый вид питомца имеет свою категорию товаров
CREATE TABLE dim_product_category (
    category_id   SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    pet_category  VARCHAR(100),
    UNIQUE (category_name, pet_category)
);


-- Измерение: Товары
CREATE TABLE dim_product (
    product_id   SERIAL PRIMARY KEY,
    product_name VARCHAR(200),
    price        NUMERIC(10,2),
    quantity     INTEGER,
    weight       NUMERIC(10,2),
    color        VARCHAR(50),
    size         VARCHAR(50),
    brand        VARCHAR(100),
    material     VARCHAR(100),
    description  TEXT,
    rating       NUMERIC(3,1),
    reviews      INTEGER,
    release_date DATE,
    expiry_date  DATE,
    category_id  INTEGER REFERENCES dim_product_category(category_id)
);


-- Измерение: Магазины
CREATE TABLE dim_store (
    store_id   SERIAL PRIMARY KEY,
    store_name VARCHAR(200),
    location   VARCHAR(200),
    city       VARCHAR(100),
    state      VARCHAR(100),
    phone      VARCHAR(50),
    email      VARCHAR(200),
    country_id INTEGER REFERENCES dim_country(country_id)
);


-- Измерение: Поставщики
CREATE TABLE dim_supplier (
    supplier_id   SERIAL PRIMARY KEY,
    supplier_name VARCHAR(200),
    contact       VARCHAR(200),
    email         VARCHAR(200),
    phone         VARCHAR(50),
    address       VARCHAR(200),
    city          VARCHAR(100),
    country_id    INTEGER REFERENCES dim_country(country_id)
);


-- Измерение: Дата
-- Позволяет анализировать продажи по периодам без вычислений
CREATE TABLE dim_date (
    date_id      SERIAL PRIMARY KEY,
    full_date    DATE NOT NULL UNIQUE,
    day          SMALLINT,
    month        SMALLINT,
    year         SMALLINT,
    quarter      SMALLINT,
    week_of_year SMALLINT
);


-- Таблица фактов: Продажи
-- Центральная таблица схемы снежинка — только числовые меры
-- и внешние ключи к измерениям
CREATE TABLE fact_sales (
    sale_id          SERIAL PRIMARY KEY,
    date_id          INTEGER NOT NULL REFERENCES dim_date(date_id),
    customer_id      INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    seller_id        INTEGER NOT NULL REFERENCES dim_seller(seller_id),
    product_id       INTEGER NOT NULL REFERENCES dim_product(product_id),
    store_id         INTEGER          REFERENCES dim_store(store_id),
    supplier_id      INTEGER          REFERENCES dim_supplier(supplier_id),
    sale_quantity    INTEGER,
    sale_total_price NUMERIC(10,2)
);
