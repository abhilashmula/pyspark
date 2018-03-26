import os
from pyspark import SparkConf, SparkContext

# Configure the environment
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/hdp/2.6.3.0-235/spark2/'

conf = SparkConf().setAppName('pyspark01').setMaster('local[32]')
sc = SparkContext(conf=conf)

if __name__ == '__main__':
    ls = range(100)
    ls_rdd = sc.parallelize(ls, numSlices=1000)
    ls_out = ls_rdd.map(lambda x: x+1).collect()
    print 'output!: ', ls_out
