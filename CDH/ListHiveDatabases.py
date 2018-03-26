from pyspark import SQLContext, SparkConf, SparkContext , HiveContext
import os

os.environ["HADOOP_USER_NAME"] = "infa_cdh"
conf = SparkConf().setAppName('ListDatabasesinHive')
HiveContext.setConf(("hive.metastore.uris", "thrift://METASTORE:9083")
sc = SparkContext(conf=conf)

for item in sorted(sc._conf.getAll()): print(item)

#sqlSparkContext = HiveContext(sc)
sqlContext = HiveContext(sc)
FromSQL = sqlContext.sql("show databases")
FromSQL.show()
