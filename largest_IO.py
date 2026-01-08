"""
Headers of the CSV file:
uid,jobid,rank,recordid,fstype,POSIX_OPENS,POSIX_FILENOS,POSIX_DUPS,POSIX_READS,POSIX_WRITES,POSIX_SEEKS,POSIX_STATS,POSIX_MMAPS,POSIX_FSYNCS,
POSIX_FDSYNCS,POSIX_BYTES_READ,POSIX_BYTES_WRITTEN,POSIX_CONSEC_READS,POSIX_CONSEC_WRITES,POSIX_SEQ_READS,POSIX_SEQ_WRITES,POSIX_SIZE_READ_0_100,
POSIX_SIZE_READ_100_1K,POSIX_SIZE_READ_1K_10K,POSIX_SIZE_READ_10K_100K,POSIX_SIZE_READ_100K_1M,POSIX_SIZE_READ_1M_4M,POSIX_SIZE_READ_4M_10M,
POSIX_SIZE_READ_10M_100M,POSIX_SIZE_READ_100M_1G,POSIX_SIZE_READ_1G_PLUS,POSIX_SIZE_WRITE_0_100,POSIX_SIZE_WRITE_100_1K,POSIX_SIZE_WRITE_1K_10K,
POSIX_SIZE_WRITE_10K_100K,POSIX_SIZE_WRITE_100K_1M,POSIX_SIZE_WRITE_1M_4M,POSIX_SIZE_WRITE_4M_10M,POSIX_SIZE_WRITE_10M_100M,POSIX_SIZE_WRITE_100M_1G,
POSIX_SIZE_WRITE_1G_PLUS,POSIX_F_OPEN_START_TIMESTAMP,POSIX_F_READ_START_TIMESTAMP,POSIX_F_WRITE_START_TIMESTAMP,POSIX_F_CLOSE_START_TIMESTAMP,
POSIX_F_OPEN_END_TIMESTAMP,POSIX_F_READ_END_TIMESTAMP,POSIX_F_WRITE_END_TIMESTAMP,POSIX_F_CLOSE_END_TIMESTAMP,POSIX_F_READ_TIME,POSIX_F_WRITE_TIME,
POSIX_F_META_TIME,MPIIO_INDEP_OPENS,MPIIO_COLL_OPENS,MPIIO_INDEP_READS,MPIIO_INDEP_WRITES,MPIIO_COLL_READS,MPIIO_COLL_WRITES,MPIIO_SPLIT_READS,
MPIIO_SPLIT_WRITES,MPIIO_NB_READS,MPIIO_NB_WRITES,MPIIO_SYNCS,MPIIO_MODE,MPIIO_BYTES_READ,MPIIO_BYTES_WRITTEN,MPIIO_RW_SWITCHES,MPIIO_SIZE_READ_AGG_0_100,
MPIIO_SIZE_READ_AGG_100_1K,MPIIO_SIZE_READ_AGG_1K_10K,MPIIO_SIZE_READ_AGG_10K_100K,MPIIO_SIZE_READ_AGG_100K_1M,MPIIO_SIZE_READ_AGG_1M_4M,
MPIIO_SIZE_READ_AGG_4M_10M,MPIIO_SIZE_READ_AGG_10M_100M,MPIIO_SIZE_READ_AGG_100M_1G,MPIIO_SIZE_READ_AGG_1G_PLUS,MPIIO_SIZE_WRITE_AGG_0_100,
MPIIO_SIZE_WRITE_AGG_100_1K,MPIIO_SIZE_WRITE_AGG_1K_10K,MPIIO_SIZE_WRITE_AGG_10K_100K,MPIIO_SIZE_WRITE_AGG_100K_1M,MPIIO_SIZE_WRITE_AGG_1M_4M,
MPIIO_SIZE_WRITE_AGG_4M_10M,MPIIO_SIZE_WRITE_AGG_10M_100M,MPIIO_SIZE_WRITE_AGG_100M_1G,MPIIO_SIZE_WRITE_AGG_1G_PLUS,MPIIO_F_OPEN_START_TIMESTAMP,
MPIIO_F_READ_START_TIMESTAMP,MPIIO_F_WRITE_START_TIMESTAMP,MPIIO_F_CLOSE_START_TIMESTAMP,MPIIO_F_OPEN_END_TIMESTAMP,MPIIO_F_READ_END_TIMESTAMP,
MPIIO_F_WRITE_END_TIMESTAMP,MPIIO_F_CLOSE_END_TIMESTAMP,MPIIO_F_READ_TIME,MPIIO_F_WRITE_TIME,MPIIO_F_META_TIME,MPIIO_F_VARIANCE_RANK_TIME,
MPIIO_F_VARIANCE_RANK_BYTES,STDIO_OPENS,STDIO_FDOPENS,STDIO_READS,STDIO_WRITES,STDIO_SEEKS,STDIO_STATS,STDIO_FLUSHES,STDIO_BYTES_READ,
STDIO_BYTES_WRITTEN,STDIO_META_TIME,STDIO_READ_TIME,STDIO_WRITE_TIME
"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from timeit import default_timer as timer
from math import ceil

conf = SparkConf().setAppName("projet")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

start = timer()

lines_df = spark.read.load("/user/fzanonboito/CISD/darshan/9/Darshan.csv", format = "csv", header = "true", inferSchema = "true")
for i in range(5, len(lines_df.columns)):
    lines_df = lines_df.withColumn(lines_df.columns[i], lines_df[lines_df.columns[i]].cast("double"))
lines_df = lines_df.cache()
# We only consider files with fstype field set as "lustre"
lines_df = lines_df.filter(lines_df.fstype == "lustre")

lines_df = lines_df.withColumn("data_amount", F.col("POSIX_BYTES_READ") + F.col("POSIX_BYTES_WRITTEN"))
lines_df = lines_df.cache()
# regroup by jobid and sum data_amount
job_data_df = lines_df.groupBy("jobid").agg(F.sum("data_amount").alias("total_data_amount"))
job_data_df = job_data_df.cache() # Will be reused
# get top 10% jobs by data amount
total_jobs = job_data_df.count()
top_n = ceil(total_jobs * 0.1)
largest_jobs_IO_df = job_data_df.orderBy(F.desc("total_data_amount")).limit(top_n)
largest_jobs_IO_df.show()

# Create a DataFrame with the top 10% users that access the most data
user_data_df = lines_df.groupBy("uid").agg(F.sum("data_amount").alias("total_data_amount"))
user_data_df = user_data_df.cache()
total_users = user_data_df.count()
top_n_users = ceil(total_users * 0.1)
largest_users_IO_df = user_data_df.orderBy(F.desc("total_data_amount")).limit(top_n_users)
# get all jobs from these users
largest_users_jobs_df = lines_df.join(largest_users_IO_df, on="uid", how="inner").select("jobid").distinct()
largest_users_jobs_df = largest_users_jobs_df.cache()


end = timer()
print(end - start)