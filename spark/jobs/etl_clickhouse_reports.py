from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, dense_rank, when, lag, concat_ws
)
from pyspark.sql.window import Window

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
        .getOrCreate()

    fact_sales = read_postgres_table(spark, "fact_sales")
    dim_customer = read_postgres_table(spark, "dim_customer")
    dim_product = read_postgres_table(spark, "dim_product")
    dim_store = read_postgres_table(spark, "dim_store")
    dim_supplier = read_postgres_table(spark, "dim_supplier")
    dim_date = read_postgres_table(spark, "dim_date")

    # 1. Product sales: top-10, category revenue, rating/reviews
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
        .join(category_revenue, "category")

    report_product_sales = product_sales \
        .withColumn("product_rank", dense_rank().over(Window.orderBy(col("total_revenue").desc()))) \
        .filter(col("product_rank") <= 10) \
        .select(
            "product_rank",
            "product_name",
            "category",
            "total_quantity",
            "total_revenue",
            "category_revenue",
            "avg_rating",
            "reviews_count"
        )

    write_clickhouse(report_product_sales, "analytics.report_product_sales")
    print("report_product_sales done")

    # 2. Customer sales: top-10, country distribution, average check
    country_customers = dim_customer.groupBy("country") \
        .agg(count("*").alias("country_customers_count"))

    customer_sales = fact_sales.join(dim_customer, "customer_id") \
        .groupBy("customer_id", "first_name", "last_name", "email", "country") \
        .agg(
            count("*").alias("total_orders"),
            sum("total_price").alias("total_spent"),
            avg("total_price").alias("avg_check")
        ) \
        .join(country_customers, "country")

    report_customer_sales = customer_sales \
        .withColumn("customer_rank", dense_rank().over(Window.orderBy(col("total_spent").desc()))) \
        .filter(col("customer_rank") <= 10) \
        .withColumn("customer_name", concat_ws(" ", col("first_name"), col("last_name"))) \
        .select(
            "customer_rank",
            "customer_id",
            "customer_name",
            "email",
            "country",
            "country_customers_count",
            "total_orders",
            "total_spent",
            "avg_check"
        )

    write_clickhouse(report_customer_sales, "analytics.report_customer_sales")
    print("report_customer_sales done")

    # 3. Time sales: monthly/yearly trends, period comparison, avg order size
    yearly_revenue = fact_sales.join(dim_date, "date_id") \
        .groupBy("year") \
        .agg(sum("total_price").alias("yearly_revenue"))

    monthly_sales = fact_sales.join(dim_date, "date_id") \
        .groupBy("year", "month") \
        .agg(
            sum("total_price").alias("monthly_revenue"),
            avg("quantity").alias("avg_order_size")
        ) \
        .join(yearly_revenue, "year")

    report_time_sales = monthly_sales \
        .withColumn("previous_month_revenue", lag("monthly_revenue").over(Window.orderBy("year", "month"))) \
        .withColumn(
            "previous_month_revenue",
            when(col("previous_month_revenue").isNull(), 0).otherwise(col("previous_month_revenue"))
        ) \
        .withColumn("revenue_diff", col("monthly_revenue") - col("previous_month_revenue")) \
        .select(
            "year",
            "month",
            "monthly_revenue",
            "yearly_revenue",
            "avg_order_size",
            "previous_month_revenue",
            "revenue_diff"
        )

    write_clickhouse(report_time_sales, "analytics.report_time_sales")
    print("report_time_sales done")

    # 4. Store sales: top-5, city/country distribution, avg check
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
        .join(city_revenue, "city") \
        .join(country_store_revenue, "country")

    report_store_sales = store_sales \
        .withColumn("store_rank", dense_rank().over(Window.orderBy(col("store_revenue").desc()))) \
        .filter(col("store_rank") <= 5) \
        .select(
            "store_rank",
            "store_id",
            "store_name",
            "city",
            "country",
            "city_revenue",
            "country_revenue",
            "store_revenue",
            "avg_check"
        )

    write_clickhouse(report_store_sales, "analytics.report_store_sales")
    print("report_store_sales done")

    # 5. Supplier sales: top-5, avg product price, country distribution
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
        .join(supplier_country_revenue, "country")

    report_supplier_sales = supplier_sales \
        .withColumn("supplier_rank", dense_rank().over(Window.orderBy(col("supplier_revenue").desc()))) \
        .filter(col("supplier_rank") <= 5) \
        .select(
            "supplier_rank",
            "supplier_id",
            "supplier_name",
            "country",
            "supplier_revenue",
            "avg_product_price",
            "country_revenue"
        )

    write_clickhouse(report_supplier_sales, "analytics.report_supplier_sales")
    print("report_supplier_sales done")

    # 6. Product quality: high/low rating, reviews rank, relation between rating and sales
    quality = fact_sales.join(dim_product, "product_id") \
        .groupBy("product_name", "category") \
        .agg(
            avg("review_rating").alias("avg_rating"),
            count("*").alias("reviews_count"),
            sum("quantity").alias("total_quantity"),
            sum("total_price").alias("total_revenue")
        )

    report_product_quality = quality \
        .withColumn("reviews_rank", dense_rank().over(Window.orderBy(col("reviews_count").desc()))) \
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
            "reviews_rank",
            "total_quantity",
            "total_revenue",
            "rating_sales_relation"
        )

    write_clickhouse(report_product_quality, "analytics.report_product_quality")
    print("report_product_quality done")

    spark.stop()


if __name__ == "__main__":
    main()