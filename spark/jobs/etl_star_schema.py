from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth

POSTGRES_URL = "jdbc:postgresql://postgres:5432/postgres"
JAR_PATH = "/opt/jars/postgresql-42.7.3.jar"


def read_table(spark, table_name):
    return spark.read.format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()


def write_table(df, table_name, mode="append"):
    df.write.format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode(mode) \
        .save()


def main():
    spark = SparkSession.builder \
        .appName("ETL Star Schema") \
        .config("spark.jars", JAR_PATH) \
        .getOrCreate()

    raw_df = read_table(spark, "raw_data")

    print("RAW COUNT:", raw_df.count())

    dim_customer = raw_df.select(
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("customer_country").alias("country"),
        col("customer_postal_code").alias("postal_code")
    ).dropDuplicates(["email"])

    write_table(dim_customer, "dim_customer")
    print("dim_customer written:", dim_customer.count())

    dim_seller = raw_df.select(
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_country").alias("country"),
        col("seller_postal_code").alias("postal_code")
    ).dropDuplicates(["email"])

    write_table(dim_seller, "dim_seller")
    print("dim_seller written:", dim_seller.count())

    dim_product = raw_df.select(
        col("product_name"),
        col("product_category").alias("category"),
        col("product_price").alias("price"),
        col("product_quantity").alias("stock_quantity"),
        col("product_rating").alias("review_rating"),
        col("product_description").alias("review_comment")
    ).dropDuplicates([
        "product_name",
        "category",
        "price",
        "stock_quantity",
        "review_rating",
        "review_comment"
    ])

    write_table(dim_product, "dim_product")
    print("dim_product written:", dim_product.count())

    dim_store = raw_df.select(
        col("store_name"),
        col("store_location").alias("location"),
        col("store_city").alias("city"),
        col("store_state").alias("state"),
        col("store_country").alias("country"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email")
    ).dropDuplicates(["email"])

    write_table(dim_store, "dim_store")
    print("dim_store written:", dim_store.count())

    dim_supplier = raw_df.select(
        col("supplier_name"),
        col("supplier_contact").alias("contact_name"),
        col("supplier_country").alias("country"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone")
    ).dropDuplicates(["email"])

    write_table(dim_supplier, "dim_supplier")
    print("dim_supplier written:", dim_supplier.count())

    dim_date = raw_df.select(
        col("sale_date")
    ).dropDuplicates(["sale_date"]) \
        .withColumn("year", year(col("sale_date"))) \
        .withColumn("month", month(col("sale_date"))) \
        .withColumn("day", dayofmonth(col("sale_date")))

    write_table(dim_date, "dim_date")
    print("dim_date written:", dim_date.count())

    customers = read_table(spark, "dim_customer")
    sellers = read_table(spark, "dim_seller")
    products = read_table(spark, "dim_product")
    stores = read_table(spark, "dim_store")
    suppliers = read_table(spark, "dim_supplier")
    dates = read_table(spark, "dim_date")

    fact_sales = raw_df \
        .join(customers, raw_df.customer_email == customers.email, "left") \
        .join(sellers, raw_df.seller_email == sellers.email, "left") \
        .join(stores, raw_df.store_email == stores.email, "left") \
        .join(suppliers, raw_df.supplier_email == suppliers.email, "left") \
        .join(
            products,
            (raw_df.product_name == products.product_name) &
            (raw_df.product_category == products.category) &
            (raw_df.product_price == products.price) &
            (raw_df.product_quantity == products.stock_quantity) &
            (raw_df.product_rating == products.review_rating) &
            (raw_df.product_description == products.review_comment),
            "left"
        ) \
        .join(dates, raw_df.sale_date == dates.sale_date, "left") \
        .select(
            customers.customer_id.alias("customer_id"),
            sellers.seller_id.alias("seller_id"),
            products.product_id.alias("product_id"),
            stores.store_id.alias("store_id"),
            suppliers.supplier_id.alias("supplier_id"),
            dates.date_id.alias("date_id"),
            raw_df.sale_quantity.alias("quantity"),
            raw_df.sale_total_price.alias("total_price")
        )

    write_table(fact_sales, "fact_sales")
    print("fact_sales written:", fact_sales.count())

    spark.stop()


if __name__ == "__main__":
    main()