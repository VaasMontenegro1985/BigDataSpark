DROP DATABASE IF EXISTS analytics;

CREATE DATABASE analytics;

CREATE TABLE analytics.report_product_sales (
    product_rank UInt8,
    product_name String,
    category String,
    total_quantity UInt64,
    total_revenue Float64,
    category_revenue Float64,
    avg_rating Float64,
    reviews_count UInt64
)
ENGINE = MergeTree()
ORDER BY product_rank;

CREATE TABLE analytics.report_customer_sales (
    customer_rank UInt8,
    customer_id UInt64,
    customer_name String,
    email String,
    country String,
    country_customers_count UInt64,
    total_orders UInt64,
    total_spent Float64,
    avg_check Float64
)
ENGINE = MergeTree()
ORDER BY customer_rank;

CREATE TABLE analytics.report_time_sales (
    year UInt16,
    month UInt8,
    monthly_revenue Float64,
    yearly_revenue Float64,
    avg_order_size Float64,
    previous_month_revenue Float64,
    revenue_diff Float64
)
ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE analytics.report_store_sales (
    store_rank UInt8,
    store_id UInt64,
    store_name String,
    city String,
    country String,
    city_revenue Float64,
    country_revenue Float64,
    store_revenue Float64,
    avg_check Float64
)
ENGINE = MergeTree()
ORDER BY store_rank;

CREATE TABLE analytics.report_supplier_sales (
    supplier_rank UInt8,
    supplier_id UInt64,
    supplier_name String,
    country String,
    supplier_revenue Float64,
    avg_product_price Float64,
    country_revenue Float64
)
ENGINE = MergeTree()
ORDER BY supplier_rank;

CREATE TABLE analytics.report_product_quality (
    product_name String,
    category String,
    avg_rating Float64,
    rating_position String,
    reviews_count UInt64,
    reviews_rank UInt64,
    total_quantity UInt64,
    total_revenue Float64,
    rating_sales_relation Float64
)
ENGINE = MergeTree()
ORDER BY avg_rating;