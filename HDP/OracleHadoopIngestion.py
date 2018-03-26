import os
from pyspark import SparkConf, SparkContext,SQLContext
from pyspark.sql import HiveContext, SQLContext

conf = (SparkConf()
        .setAppName("oracle_data_import")
        .set("spark.dynamicAllocation.enabled", "true")
        .set("spark.shuffle.service.enabled", "true"))

sc = SparkContext(conf=conf)

sqlctx = SQLContext(sc)

#df = sqlctx.read.jdbc(url="jdbc:oracle:thin:c##_demo01/infa@//192.168.1.142:1521/orcl",table="orders")

df = sqlctx.read.format("jdbc").options(driver="oracle.jdbc.driver.OracleDriver", url="jdbc:oracle:thin:@//192.168.1.142:1521/orcl", dbtable="orders",user="c##demo01", password="infa").load()

## this is to write to an ORC file
df.write.format("orc").save("/tmp/orc_query_output")

## this is to write to a hive table
df.write.mode('overwrite').format('orc').saveAsTable("pyspark_orcl_orders")

