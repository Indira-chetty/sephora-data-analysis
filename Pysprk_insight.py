from pyspark.sql import SparkSession
import os

# Set Hadoop Environment Variables
os.environ["HADOOP_HOME"] = "C:\\Users\\Owner\\Downloads\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\Users\\Owner\\Downloads\\hadoop"

# Path to the JDBC driver
jdbc_driver_path = "C:/Program Files/Spark/spark-3.5.4-bin-hadoop3/jars/mssql-jdbc-12.8.1.jre11.jar"

# Set authentication DLL path (Change this to match your downloaded version)
auth_dll_path = "C:/Program Files/Microsoft_JDBC_Driver/sqljdbc_12.8.1.0_enu (1)/sqljdbc_12.8/enu/auth/x64/mssql-jdbc_auth-12.8.1.x64.dll"

# Create Spark Session with DLL Authentication Path
spark = SparkSession.builder \
    .appName("PySparkSQLServerConnection") \
    .config("spark.jars", jdbc_driver_path) \
    .config("spark.driver.extraClassPath", jdbc_driver_path) \
    .config("spark.driver.extraLibraryPath", auth_dll_path) \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

print("✅ PySpark Session Started Successfully!")

# JDBC connection URL for SQL Server (Windows Authentication)
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=SephoraProductReview;integratedSecurity=true;trustServerCertificate=true"

# Connection properties
db_properties = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Load Data from SQL Server into PySpark DataFrame
df_sephora = spark.read.jdbc(url=jdbc_url, table="dbo.CombinedfinalTable", properties=db_properties)

# Show Schema and Sample Data
df_sephora.printSchema()
df_sephora.show(5)


from pyspark.sql.functions import col, avg, count , when

print("1.Top 10 Highest Rated Products")
df_sephora.groupBy("product_name", "brand_name") \
    .agg(avg("rating").alias("avg_rating"), count("*").alias("review_count")) \
    .filter(col("review_count") > 50) \
    .orderBy(col("avg_rating").desc()) \
    .limit(10) \
    .show()

print("2.Top Rated Brands")
df_sephora.groupBy("brand_name") \
    .agg(avg("rating").alias("avg_rating"), count("*").alias("review_count")) \
    .filter(col("review_count") > 100) \
    .orderBy(col("avg_rating").desc()) \
    .limit(10) \
    .show()

print("3.Products That Received the Most Negative Feedback")
df_sephora.groupBy("product_name", "brand_name") \
    .agg(count("total_neg_feedback_count").alias("total_negative_feedback")) \
    .orderBy(col("total_negative_feedback").desc()) \
    .limit(10) \
    .show()

print("4.Most Common Complaints in Negative Reviews")
df_sephora.filter(col("CustomerSentiment") == "Negative") \
    .groupBy("TopRated") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

print("5.Price Impact on Customer Sentiment")
df_sephora.filter(col("CustomerSentiment") == "Negative") \
    .groupBy(
        when(col("price_usd") < 20, "Budget (Under $20)")
        .when((col("price_usd") >= 20) & (col("price_usd") <= 50), "Mid-Range ($20-$50)")
        .otherwise("Luxury (Above $50)")
        .alias("price_category")
    ) \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

print("6.Are Negative Reviews Due to Stock or Availability Issues?")
df_sephora.filter((col("CustomerSentiment") == "Negative") & (col("out_of_stock") == 1)) \
    .groupBy("product_name", "brand_name") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

print("7.Are Negative Reviews Related to Online-Only Products?")
df_sephora.filter(col("CustomerSentiment") == "Negative") \
    .groupBy("online_only") \
    .count() \
    .show()

print("8.Find Products That Are Most Recommended Despite Negative Reviews")
df_sephora.filter(col("CustomerSentiment") == "Negative") \
    .groupBy("product_name", "brand_name") \
    .agg(
        count("*").alias("total_negative_reviews"),
        count(when(col("is_recommended") == 1, 1)).alias("total_recommendations")
    ) \
    .orderBy(col("total_negative_reviews").desc()) \
    .show()

print("9.Products That Are Mostly Out of Stock")
df_sephora.filter(col("out_of_stock") == 1) \
    .groupBy("product_name", "brand_name") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(10) \
    .show()

print("10.Customer Sentiment Analysis")
df_sephora.filter(col("CustomerSentiment").isNotNull()) \
    .groupBy("CustomerSentiment") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

print("11.Sephora Exclusive Products vs. Non-Exclusive")
df_sephora.groupBy("sephora_exclusive") \
    .agg(count("*").alias("total_products"), avg("rating").alias("avg_rating")) \
    .show()

print("12.Most Recommended Products")
df_sephora.filter(col("is_recommended") == 1) \
    .groupBy("product_name", "brand_name") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(10) \
    .show()
