from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PostgresConnectionTest") \
    .config("spark.jars", "/opt/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
    .option("dbtable", "raw_data") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

print("ROWS COUNT:", df.count())

df.show(5)

spark.stop()