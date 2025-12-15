# TODO : Create a RDD with the top 10% longest jobs in execution time

from pyspark import SparkContext, SparkConf
from timeit import default_timer as timer

conf = SparkConf().setAppName("projet")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

start = timer()

lines=sc.textFile("/user/fzanonboito/CISD/darshan/9/Darshan.csv")

end = timer()
print(end - start)
