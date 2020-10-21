
from pyspark.sql import functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("filtered_comments.parquet")
df = df.repartition(1)
df = df.sortWithinPartitions(["subreddit","CreatedAt","id"])
df.write.parquet("covid-19_reddit_comments.parquet",mode='overwrite')
df.write.json("covid-19_reddit_comments.json",mode='overwrite')
