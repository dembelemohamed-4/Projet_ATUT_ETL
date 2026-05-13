from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when

#TANSFORMATION
def create_spark_sessions ():
    "create an session spark"

    return (
        SparkSession.builder \
    .appName("ETL-MinIO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.slf4j:slf4j-reload4j:1.7.36") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "user") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
    )

def transform_dataframe(spark, path: str, source: str):
    """Loads and transforms a book dataset based on its source.."""

    df = spark.read.csv(
        path,
        header=True,
        inferSchema=False
    )

    if source == "web":
        df = (
            df
            .withColumn("source", lit("web"))
            .withColumn("auteur", lit(None))
            .withColumn("ISBN", lit(None))
        )

        df = df.select(
            col("ISBN"),
            col("title").alias("titre"),
            col("auteur"),
            col("price").alias("prix"),
            when(col("rating") == "One", 1)
            .when(col("rating") == "Two", 2)
            .when(col("rating") == "Three", 3)
            .when(col("rating") == "Four", 4)
            .when(col("rating") == "Five", 5)
            .otherwise(None)
            .alias("note"),
            col("category").alias("categorie"),
            col("link").alias("lien_du_livre"),
            col("image_link").alias("lien_image"),
            col("source"),
        )

    elif source == "csv":
        df = (
            df.withColumn("source", lit("csv"))
            .withColumn("note", lit(None))
            .withColumn("categorie", lit(None))
            .withColumn("prix", lit(None))
            .withColumn("lien_du_livre", lit(None))
        )

        df = df.select(
            col("ISBN"),
            col("Book-Title").alias("titre"),
            col("Book-Author").alias("auteur"),
            col("prix"),
            col("note"),
            col("categorie"),
            col("lien_du_livre"),
            col("Image-URL-L").alias("lien_image"),
            col("source"),
        )

    else:
        print("source_type doit être 'web' ou 'csv'")

    return df

def transformation():
    
    spark = create_spark_sessions()

    df_web = transform_dataframe(
        spark,
        path="s3a://books/raw_data/scraping_books.csv",
        source="web"
    )

    df_csv = transform_dataframe(
        spark,
        path="s3a://books/raw_data/Books.csv",
        source="csv"
    )

    df_final = df_web.union(df_csv)
    df_final.write \
        .format("csv") \
        .mode("overwrite") \
        .option("header", True) \
        .save("s3a://books/clean_data")

    print("Données stockées dans MinIO ✅")