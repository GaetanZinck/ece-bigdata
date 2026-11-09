import os
from pyspark.sql import SparkSession

BUCKET = os.environ["LAB_BUCKET_NAME"]
VERSION = os.environ["METADATA_VERSION"].strip()
spark = (
    SparkSession.builder
    .appName("lab-polaris")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION"))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", f"{os.environ['POLARIS_URI']}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", os.environ["POLARIS_CATALOG"])
    .config("spark.sql.catalog.polaris.credential", os.environ["POLARIS_DEV_CREDENTIAL"])
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
tables = [r.tableName for r in spark.sql("SHOW TABLES IN polaris.silver").collect()]
if "orders" not in tables:
    spark.sql(f"""
        CALL polaris.system.register_table(
          table => 'silver.orders',
          metadata_file => 's3a://{BUCKET}/warehouse/silver/orders/metadata/v{VERSION}.metadata.json'
        )
    """).show(truncate=False)
else:
    print("silver.orders déjà enregistrée dans Polaris")
if "users" not in tables:
    spark.table("lab.silver.users").writeTo("polaris.silver.users").createOrReplace()

o = spark.table("polaris.silver.orders").count()
u = spark.table("polaris.silver.users").count()
print(f"OK Polaris : silver.orders={o} (attendu 2829), silver.users={u} (attendu 50)")
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.stop()
