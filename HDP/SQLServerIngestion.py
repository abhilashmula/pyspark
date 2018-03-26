from pyspark.sql import SparkSession

### Database details
url = "jdbc:sqlserver://infaws\infa:1433;databaseName=Demo01;user=sa;password=infa123!@#"
databasename = "demo01"
tablelist = ["customers","distributors","orders","employees"]

spark = SparkSession.builder.getOrCreate()


for tablename in tablelist:
    print(30 * '-')
    print(tablename)
    print(30 * '-')
    dataframe = spark.read.format('jdbc').options(
    url = url,
    database=databasename,
    dbtable=tablename).load()
    dataframe.show()