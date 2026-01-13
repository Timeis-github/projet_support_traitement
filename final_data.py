# TODO : create a RDD with all the jobs from the 10% users that access the largest amount of data.

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

# Largest execution time jobs
# execution time is the difference between the minimum of all _TIMESTAMP fields and the maximum of all _TIMESTAMP fields of all rows with the same jobid
timestamp_columns = [col for col in lines_df.columns if col.endswith("_TIMESTAMP")]
min_timestamp = F.least(*[F.col(col) for col in timestamp_columns])
max_timestamp = F.greatest(*[F.col(col) for col in timestamp_columns])
job_times_df = lines_df.withColumn("min_timestamp", min_timestamp).withColumn("max_timestamp", max_timestamp)
job_times_df = job_times_df.withColumn("execution_time", F.col("max_timestamp") - F.col("min_timestamp"))
job_execution_times_df = job_times_df.groupBy("jobid").agg(F.min("min_timestamp").alias("job_start"), F.max("max_timestamp").alias("job_end"))
job_execution_times_df = job_execution_times_df.withColumn("execution_time", F.col("job_end") - F.col("job_start"))
total_jobs = job_execution_times_df.count()
top_n = ceil(total_jobs * 0.1)
longest_jobs_df = job_execution_times_df.orderBy(F.desc("execution_time")).limit(top_n)

# Largest I/O jobs
# We only consider files with fstype field set as "lustre"
lustre_df = lines_df.filter(lines_df.fstype == "lustre")

file_data_df = lustre_df.withColumn("data_amount", F.col("POSIX_BYTES_READ") + F.col("POSIX_BYTES_WRITTEN"))
file_data_df = file_data_df.cache()
# regroup by jobid and sum data_amount
job_data_df = file_data_df.groupBy("jobid").agg(F.sum("data_amount").alias("total_data_amount"))
job_data_df = job_data_df.cache() # Will be reused
# get top 10% jobs by data amount
total_jobs = job_data_df.count()
top_n = ceil(total_jobs * 0.1)
largest_jobs_IO_df = job_data_df.orderBy(F.desc("total_data_amount")).limit(top_n)

# Users with largest I/O
# Create a DataFrame with the top 10% users that access the most data
user_data_df = file_data_df.groupBy("uid").agg(F.sum("data_amount").alias("total_data_amount"))
user_data_df = user_data_df.cache()
total_users = user_data_df.count()
top_n = ceil(total_users * 0.1)
largest_users_IO_df = user_data_df.orderBy(F.desc("total_data_amount")).limit(top_n)
# get all jobs from these users
largest_users_jobs_df = lines_df.join(largest_users_IO_df, on="uid", how="inner").select("jobid").distinct()
largest_users_jobs_df = largest_users_jobs_df.cache()

# Gather all jobids from the three previous results
longest_jobids_df = longest_jobs_df.select("jobid")
largest_jobs_IO_jobids_df = largest_jobs_IO_df.select("jobid")
largest_users_jobids_df = largest_users_jobs_df.select("jobid")
jobids_df = longest_jobids_df.union(largest_jobs_IO_jobids_df).union(largest_users_jobids_df).distinct()


final_df = jobids_df


# Compare with existing results
df_result = spark.read.option("header", "false").option("inferSchema", "true").csv("/user/fzanonboito/CISD/topjobs/topjobs_9/*")
df_result.show()

jobids_df.distinct().show()
jobids_df.distinct().show()


end = timer()
print("Time : " + str(end - start))
