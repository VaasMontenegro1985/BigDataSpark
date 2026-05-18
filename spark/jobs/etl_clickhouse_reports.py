from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, when, concat_ws,
    year, month
)

POSTGRES_URL = "jdbc:postgresql://postgres:5432/postgres"
CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/analytics"

JARS = "/opt/jars/postgresql-42.7.3.jar,/opt/jars/clickhouse-jdbc.jar"


def read_postgres_table(spark, table_name):
    return spark.read.format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()


def write_clickhouse(df, table_name):
    df.write.format("jdbc") \
        .option("url", CLICKHOUSE_URL) \
        .option("dbtable", table_name) \
        .option("user", "default") \
        .option("password", "") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()


def main():
    spark = SparkSession.builder \
        .appName("ClickHouse Reports ETL") \
        .config("spark.jars", JARS) \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    fact_sales = read_postgres_table(spark, "fact_sales")
    dim_customer = read_postgres_table(spark, "dim_customer")
    dim_product = read_postgres_table(spark, "dim_product")
    dim_store = read_postgres_table(spark, "dim_store")
    dim_supplier = read_postgres_table(spark, "dim_supplier")

    # 1. Product sales
    category_revenue = fact_sales.join(dim_product, "product_id") \
        .groupBy("category") \
        .agg(sum("total_price").alias("category_revenue"))

    product_sales = fact_sales.join(dim_product, "product_id") \
        .groupBy("product_name", "category") \
        .agg(
            sum("quantity").alias("total_quantity"),
            sum("total_price").alias("total_revenue"),
            avg("review_rating").alias("avg_rating"),
            count("*").alias("reviews_count")
        ) \
        .orderBy(col("total_revenue").desc()) \
        .limit(10)

    write_clickhouse(product_sales, "analytics.report_product_sales")
    write_clickhouse(category_revenue, "analytics.report_category_revenue")

    print("report_product_sales done")

    # 2. Customer sales
    country_customers = dim_customer.groupBy("country") \
        .agg(count("*").alias("country_customers_count"))

    customer_sales = fact_sales.join(dim_customer, "customer_id") \
        .groupBy("customer_id", "first_name", "last_name", "email", "country") \
        .agg(
            count("*").alias("total_orders"),
            sum("total_price").alias("total_spent"),
            avg("total_price").alias("avg_check")
        ) \
        .withColumn("customer_name", concat_ws(" ", col("first_name"), col("last_name"))) \
        .orderBy(col("total_spent").desc()) \
        .limit(10)

    write_clickhouse(customer_sales, "analytics.report_customer_sales")
    write_clickhouse(country_customers, "analytics.report_country_customers")

    print("report_customer_sales done")

    # 3. Time sales
    yearly_revenue = fact_sales \
        .withColumn("year", year(col("sale_date"))) \
        .groupBy("year") \
        .agg(sum("total_price").alias("yearly_revenue"))

    monthly_sales = fact_sales \
        .withColumn("year", year(col("sale_date"))) \
        .withColumn("month", month(col("sale_date"))) \
        .groupBy("year", "month") \
        .agg(
            sum("total_price").alias("monthly_revenue"),
            avg("quantity").alias("avg_order_size")
        ) \
        .orderBy("year", "month")

    write_clickhouse(monthly_sales, "analytics.report_time_sales")
    write_clickhouse(yearly_revenue, "analytics.report_yearly_revenue")

    print("report_time_sales done")

    # 4. Store sales
    city_revenue = fact_sales.join(dim_store, "store_id") \
        .groupBy("city") \
        .agg(sum("total_price").alias("city_revenue"))

    country_store_revenue = fact_sales.join(dim_store, "store_id") \
        .groupBy("country") \
        .agg(sum("total_price").alias("country_revenue"))

    store_sales = fact_sales.join(dim_store, "store_id") \
        .groupBy("store_id", "store_name", "city", "country") \
        .agg(
            sum("total_price").alias("store_revenue"),
            avg("total_price").alias("avg_check")
        ) \
        .orderBy(col("store_revenue").desc()) \
        .limit(5)

    write_clickhouse(store_sales, "analytics.report_store_sales")
    write_clickhouse(city_revenue, "analytics.report_city_revenue")
    write_clickhouse(country_store_revenue, "analytics.report_country_store_revenue")

    print("report_store_sales done")

    # 5. Supplier sales
    supplier_country_revenue = fact_sales.join(dim_supplier, "supplier_id") \
        .groupBy("country") \
        .agg(sum("total_price").alias("country_revenue"))

    supplier_sales = fact_sales.join(dim_supplier, "supplier_id") \
        .join(dim_product, "product_id") \
        .groupBy("supplier_id", "supplier_name", "country") \
        .agg(
            sum("total_price").alias("supplier_revenue"),
            avg("price").alias("avg_product_price")
        ) \
        .orderBy(col("supplier_revenue").desc()) \
        .limit(5)

    write_clickhouse(supplier_sales, "analytics.report_supplier_sales")
    write_clickhouse(supplier_country_revenue, "analytics.report_supplier_country_revenue")

    print("report_supplier_sales done")

    # 6. Product quality
    quality = fact_sales.join(dim_product, "product_id") \
        .groupBy("product_name", "category") \
        .agg(
            avg("review_rating").alias("avg_rating"),
            count("*").alias("reviews_count"),
            sum("quantity").alias("total_quantity"),
            sum("total_price").alias("total_revenue")
        )

    report_product_quality = quality \
        .withColumn(
            "rating_position",
            when(col("avg_rating") >= 4.5, "High")
            .when(col("avg_rating") <= 2.5, "Low")
            .otherwise("Medium")
        ) \
        .withColumn("rating_sales_relation", col("avg_rating") * col("total_quantity")) \
        .select(
            "product_name",
            "category",
            "avg_rating",
            "rating_position",
            "reviews_count",
            "total_quantity",
            "total_revenue",
            "rating_sales_relation"
        )

    write_clickhouse(report_product_quality, "analytics.report_product_quality")

    print("report_product_quality done")

    spark.stop()


if __name__ == "__main__":
    main()