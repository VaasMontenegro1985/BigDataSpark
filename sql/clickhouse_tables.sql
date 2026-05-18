DROP DATABASE IF EXISTS analytics;

CREATE DATABASE analytics;

CREATE TABLE analytics.report_product_sales (
    product_name String,
    category String,
    total_quantity UInt64,
    total_revenue Float64,
    avg_rating Float64,
    reviews_count UInt64
)
ENGINE = MergeTree()
ORDER BY total_revenue;

CREATE TABLE analytics.report_category_revenue (
    category String,
    category_revenue Float64
)
ENGINE = MergeTree()
ORDER BY category;

CREATE TABLE analytics.report_customer_sales (
    customer_id UInt64,
    first_name String,
    last_name String,
    email String,
    country String,
    total_orders UInt64,
    total_spent Float64,
    avg_check Float64,
    customer_name String
)
ENGINE = MergeTree()
ORDER BY total_spent;

CREATE TABLE analytics.report_country_customers (
    country String,
    country_customers_count UInt64
)
ENGINE = MergeTree()
ORDER BY country;

CREATE TABLE analytics.report_time_sales (
    year UInt16,
    month UInt8,
    monthly_revenue Float64,
    avg_order_size Float64
)
ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE analytics.report_yearly_revenue (
    year UInt16,
    yearly_revenue Float64
)
ENGINE = MergeTree()
ORDER BY year;

CREATE TABLE analytics.report_store_sales (
    store_id UInt64,
    store_name String,
    city String,
    country String,
    store_revenue Float64,
    avg_check Float64
)
ENGINE = MergeTree()
ORDER BY store_revenue;

CREATE TABLE analytics.report_city_revenue (
    city String,
    city_revenue Float64
)
ENGINE = MergeTree()
ORDER BY city;

CREATE TABLE analytics.report_country_store_revenue (
    country String,
    country_revenue Float64
)
ENGINE = MergeTree()
ORDER BY country;

CREATE TABLE analytics.report_supplier_sales (
    supplier_id UInt64,
    supplier_name String,
    country String,
    supplier_revenue Float64,
    avg_product_price Float64
)
ENGINE = MergeTree()
ORDER BY supplier_revenue;

CREATE TABLE analytics.report_supplier_country_revenue (
    country String,
    country_revenue Float64
)
ENGINE = MergeTree()
ORDER BY country;

CREATE TABLE analytics.report_product_quality (
    product_name String,
    category String,
    avg_rating Float64,
    rating_position String,
    reviews_count UInt64,
    total_quantity UInt64,
    total_revenue Float64,
    rating_sales_relation Float64
)
ENGINE = MergeTree()
ORDER BY avg_rating;