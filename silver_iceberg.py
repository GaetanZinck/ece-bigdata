import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

BUCKET = os.environ["LAB_BUCKET_NAME"]
spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

read = dict(header=True, inferSchema=True, samplingRatio=0.01, multiLine=True)
users_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/users.csv", **read)
orders_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/orders.csv", **read)

# stg_users : mêmes règles que le lab
users_typed = (
    users_bronze.select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)
stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
).withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))

# stg_orders : typage + dédoublonnage par fenêtre
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)
w = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = (orders_typed.withColumn("rn", F.row_number().over(w))
              .filter("rn = 1").drop("rn"))

spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()

n_users = spark.table("lab.silver.users").count()
n_orders = spark.table("lab.silver.orders").count()
assert n_users == users_bronze.count(), "users perdus"
assert n_orders == stg_orders.count(), "orders perdus"
print(f"OK silver Iceberg : users={n_users} (attendu 50), orders={n_orders} (attendu 2829)")
spark.stop()
