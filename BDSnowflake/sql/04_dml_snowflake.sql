-- 1. dim_country — справочник всех стран из исходных данных
INSERT INTO dim_country (country_name)
SELECT DISTINCT country_name
FROM (
    SELECT NULLIF(TRIM(customer_country), '') AS country_name FROM mock_data
    UNION
    SELECT NULLIF(TRIM(seller_country),   '') FROM mock_data
    UNION
    SELECT NULLIF(TRIM(store_country),    '') FROM mock_data
    UNION
    SELECT NULLIF(TRIM(supplier_country), '') FROM mock_data
) t
WHERE country_name IS NOT NULL
ORDER BY country_name;


-- 2. dim_customer — уникальные клиенты по email
INSERT INTO dim_customer (first_name, last_name, age, email, postal_code, country_id)
SELECT DISTINCT ON (customer_email)
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    NULLIF(TRIM(customer_postal_code), ''),
    c.country_id
FROM mock_data m
LEFT JOIN dim_country c ON c.country_name = NULLIF(TRIM(m.customer_country), '')
ORDER BY customer_email;


-- 3. dim_pet — питомцы, привязанные к клиентам
-- (дочернее измерение схемы снежинка)
INSERT INTO dim_pet (customer_id, pet_type, pet_name, pet_breed, pet_category)
SELECT
    dc.customer_id,
    NULLIF(TRIM(m.customer_pet_type),  ''),
    NULLIF(TRIM(m.customer_pet_name),  ''),
    NULLIF(TRIM(m.customer_pet_breed), ''),
    NULLIF(TRIM(m.pet_category),       '')
FROM (
    SELECT DISTINCT ON (customer_email)
        customer_email,
        customer_pet_type,
        customer_pet_name,
        customer_pet_breed,
        pet_category
    FROM mock_data
    ORDER BY customer_email
) m
JOIN dim_customer dc ON dc.email = m.customer_email;


-- 4. dim_seller — уникальные продавцы по email
INSERT INTO dim_seller (first_name, last_name, email, postal_code, country_id)
SELECT DISTINCT ON (seller_email)
    seller_first_name,
    seller_last_name,
    seller_email,
    NULLIF(TRIM(seller_postal_code), ''),
    c.country_id
FROM mock_data m
LEFT JOIN dim_country c ON c.country_name = NULLIF(TRIM(m.seller_country), '')
ORDER BY seller_email;


-- 5. dim_product_category — уникальные категории товаров
-- (дочернее измерение схемы снежинка)
INSERT INTO dim_product_category (category_name, pet_category)
SELECT DISTINCT
    NULLIF(TRIM(product_category), ''),
    NULLIF(TRIM(pet_category),     '')
FROM mock_data
WHERE NULLIF(TRIM(product_category), '') IS NOT NULL
ORDER BY 1;


-- 6. dim_product — уникальные товары по (name, brand, color, size, material, price)
INSERT INTO dim_product (
    product_name, price, quantity, weight, color, size, brand, material,
    description, rating, reviews, release_date, expiry_date, category_id
)
SELECT DISTINCT ON (product_name, product_brand, product_color, product_size, product_material, product_price)
    product_name,
    product_price,
    product_quantity,
    product_weight,
    NULLIF(TRIM(product_color),       ''),
    NULLIF(TRIM(product_size),        ''),
    NULLIF(TRIM(product_brand),       ''),
    NULLIF(TRIM(product_material),    ''),
    NULLIF(TRIM(product_description), ''),
    product_rating,
    product_reviews,
    TO_DATE(NULLIF(TRIM(product_release_date), ''), 'MM/DD/YYYY'),
    TO_DATE(NULLIF(TRIM(product_expiry_date),  ''), 'MM/DD/YYYY'),
    pc.category_id
FROM mock_data m
LEFT JOIN dim_product_category pc
    ON pc.category_name = NULLIF(TRIM(m.product_category), '')
   AND pc.pet_category  = NULLIF(TRIM(m.pet_category),     '')
ORDER BY product_name, product_brand, product_color, product_size, product_material, product_price;


-- 7. dim_store — уникальные магазины по (store_name, store_city)
INSERT INTO dim_store (store_name, location, city, state, phone, email, country_id)
SELECT DISTINCT ON (store_name, store_city)
    store_name,
    NULLIF(TRIM(store_location), ''),
    store_city,
    NULLIF(TRIM(store_state), ''),
    store_phone,
    store_email,
    c.country_id
FROM mock_data m
LEFT JOIN dim_country c ON c.country_name = NULLIF(TRIM(m.store_country), '')
WHERE NULLIF(TRIM(store_name), '') IS NOT NULL
ORDER BY store_name, store_city;


-- 8. dim_supplier — уникальные поставщики по (supplier_name, supplier_city)
INSERT INTO dim_supplier (supplier_name, contact, email, phone, address, city, country_id)
SELECT DISTINCT ON (supplier_name, supplier_city)
    supplier_name,
    NULLIF(TRIM(supplier_contact), ''),
    supplier_email,
    supplier_phone,
    NULLIF(TRIM(supplier_address), ''),
    supplier_city,
    c.country_id
FROM mock_data m
LEFT JOIN dim_country c ON c.country_name = NULLIF(TRIM(m.supplier_country), '')
WHERE NULLIF(TRIM(supplier_name), '') IS NOT NULL
ORDER BY supplier_name, supplier_city;


-- 9. dim_date — все уникальные даты продаж
INSERT INTO dim_date (full_date, day, month, year, quarter, week_of_year)
SELECT DISTINCT
    TO_DATE(sale_date, 'MM/DD/YYYY')                                   AS full_date,
    EXTRACT(DAY     FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::SMALLINT  AS day,
    EXTRACT(MONTH   FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::SMALLINT  AS month,
    EXTRACT(YEAR    FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::SMALLINT  AS year,
    EXTRACT(QUARTER FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::SMALLINT  AS quarter,
    EXTRACT(WEEK    FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::SMALLINT  AS week_of_year
FROM mock_data
WHERE NULLIF(TRIM(sale_date), '') IS NOT NULL;


-- 10. fact_sales — таблица фактов продаж (10 000 строк)
-- Связывает все измерения через суррогатные ключи
INSERT INTO fact_sales (
    date_id, customer_id, seller_id, product_id, store_id, supplier_id,
    sale_quantity, sale_total_price
)
SELECT
    d.date_id,
    cust.customer_id,
    sel.seller_id,
    prod.product_id,
    st.store_id,
    sup.supplier_id,
    m.sale_quantity,
    m.sale_total_price
FROM mock_data m
JOIN dim_date d
    ON d.full_date = TO_DATE(NULLIF(TRIM(m.sale_date), ''), 'MM/DD/YYYY')
JOIN dim_customer cust
    ON cust.email = m.customer_email
JOIN dim_seller sel
    ON sel.email = m.seller_email
JOIN dim_product prod
    ON prod.product_name = m.product_name
   AND prod.brand        = NULLIF(TRIM(m.product_brand),    '')
   AND prod.color        = NULLIF(TRIM(m.product_color),    '')
   AND prod.size         = NULLIF(TRIM(m.product_size),     '')
   AND prod.material     = NULLIF(TRIM(m.product_material), '')
   AND prod.price        = m.product_price
LEFT JOIN dim_store st
    ON st.store_name = m.store_name
   AND st.city       = m.store_city
LEFT JOIN dim_supplier sup
    ON sup.supplier_name = m.supplier_name
   AND sup.city          = m.supplier_city;


-- =============================================================
-- Итоговая проверка: количество строк в каждой таблице
-- =============================================================
DO $$
BEGIN
    RAISE NOTICE '=== Итог заполнения схемы снежинка ===';
    RAISE NOTICE 'dim_country:          %', (SELECT COUNT(*) FROM dim_country);
    RAISE NOTICE 'dim_customer:         %', (SELECT COUNT(*) FROM dim_customer);
    RAISE NOTICE 'dim_pet:              %', (SELECT COUNT(*) FROM dim_pet);
    RAISE NOTICE 'dim_seller:           %', (SELECT COUNT(*) FROM dim_seller);
    RAISE NOTICE 'dim_product_category: %', (SELECT COUNT(*) FROM dim_product_category);
    RAISE NOTICE 'dim_product:          %', (SELECT COUNT(*) FROM dim_product);
    RAISE NOTICE 'dim_store:            %', (SELECT COUNT(*) FROM dim_store);
    RAISE NOTICE 'dim_supplier:         %', (SELECT COUNT(*) FROM dim_supplier);
    RAISE NOTICE 'dim_date:             %', (SELECT COUNT(*) FROM dim_date);
    RAISE NOTICE 'fact_sales:           %', (SELECT COUNT(*) FROM fact_sales);
    RAISE NOTICE '=====================================';
END $$;
