import configparser
from pyspark.sql import SparkSession, functions as F
import time

spark = (SparkSession.builder
         .appName("MySQL Conn")
         # mysql java connector
         .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.32")
         .master("local[2]")
         .getOrCreate())

config = configparser.RawConfigParser()

# Mysql connection information
config.read('./db_conn')
user_name = config.get('DB', 'user_name')
password = config.get('DB', 'password')
db_ip = config.get('DB', 'db_ip')

jdbcUrl = f"jdbc:mysql://{db_ip}:3306/dataops?user={user_name}&password={password}"

# Save dataset to local
# wget -O ~/datasets/customers_train.csv <url>
# url --> https://raw.githubusercontent.com/erkansirin78/datasets/master/deltalake_streaming_demo/customers_train.csv</pre>
# This dataset consist of 1000 rows
df = (spark.read.option("header", True)
      .option("inferSchema", True)
      .csv("file:///home/train/datasets/customers_train.csv"))


# This is only a simulation. Be careful when you use collect, dataset may not fit into memory of driver
start = 0
while start < 500:
    time.sleep(2)
    end = start + 5
    df_part_list = df.collect()[start:end]
    df_part = spark.createDataFrame(df_part_list)
    df_part.show()
    df_part.write.jdbc(url=jdbcUrl,
                       table="demo",
                       mode="append",
                       # mysql driver
                       properties={"driver": "com.mysql.cj.jdbc.Driver"})
    start += 5
