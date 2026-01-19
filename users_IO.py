# TODO : create a RDD with all the jobs from the 10% users that access the largest amount of data.

from pyspark import SparkContext, SparkConf
from timeit import default_timer as timer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, desc_nulls_last, expr
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import math

conf = SparkConf().setAppName("projet")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

start = timer()

spark = SparkSession(sc)

df_result = spark.read.option("header", "false").option("inferSchema", "true").csv("/user/fzanonboito/CISD/topjobs/topjobs_9/*")
df_result.show()

## Récupération des fichiers.csv
lines_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/user/fzanonboito/CISD/darshan/9/Darshan.csv")

## Récupérations des users unique

data = lines_df.na.drop(subset=["POSIX_BYTES_READ","POSIX_BYTES_WRITTEN"])

user_df = data.select("uid","POSIX_BYTES_READ","POSIX_BYTES_WRITTEN").distinct()

## Recherche/ajout des données traiter dans un job de la part d'un user dans une colonne "total data"

user_df = user_df.withColumn("total_data",user_df["POSIX_BYTES_READ"] + user_df["POSIX_BYTES_WRITTEN"])

## Classification dans l'ordre décroissant en fonction de total data

user_df = user_df.orderBy(desc("total_data"))

## On prendre que les 10% premiers user

user_count = user_df.count()

user_count = user_count / 10
user_count = math.ceil(user_count)

user_df = user_df.limit(user_count)

## Recherche des jobs réaliser par ces users pour les mettre dans le RDD final

job_df = lines_df.filter(user_df["uid"] == lines_df["uid"]).select(lines_df["jobid"]).distinct()
job_df.show()

## RDD with all the jobs from the 10% users that access the largest amount of data


final_df = job_df

final_df = final_df.withColumn("userid", lines_df.filter(lines_df['jobid'] == job_df['jobid']).select('uid'))
final_df = final_df.withColumn("execution_time", lines_df.filter(lines_df['jobid'] == job_df['jobid']).select(expr("MAX(_T_TIMESTAMP) - MIN(_TIMESTAMP)")))
final_df = final_df.withColumn("total_number_of_files", lines_df.filter(lines_df['jobid'] == job_df['jobid'] and (lines_df["POSIX_BYTES_READ"] + lines_df["POSIX_BYTES_WRITTEN"]) > 0).select(expr("COUNT(DISTINCT POSIX_FNAME)")))

final_df.show()




end = timer()
print("Time : " + str(end - start))
