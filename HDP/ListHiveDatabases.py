from pyspark import SQLContext, SparkConf, SparkContext , HiveContext

conf = SparkConf().setAppName('ListDatabasesinHive')
sc = SparkContext(conf=conf)

sqlSparkContext = HiveContext(sc)
FromSQL = sqlSparkContext.sql("show databases")
FromSQL.show()