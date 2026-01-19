# TODO : create a RDD with all the jobs from the 10% users that access the largest amount of data.

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from timeit import default_timer as timer
from pyspark.sql.window import Window
from math import ceil
import os
import sys


#os.environ["SPARK_LOCAL_DIRS"] = "/home/llahlah/spark_tmp"
#os.environ["TMPDIR"] = "/home/llahlah/spark_tmp"

conf = SparkConf() \
    #.setAppName("projet") \
    #.set("spark.local.dir", "/home/llahlah/spark_tmp")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

start = timer()

if len(sys.argv) == 1:
    input_paths = ["/user/fzanonboito/CISD/darshan/1/Darshan.csv","/user/fzanonboito/CISD/darshan/2/Darshan.csv","/user/fzanonboito/CISD/darshan/3/Darshan.csv"]
elif len(sys.argv) < 3:
    print("Usage: spark-submit script.py <csv1> <csv2> ... <output_dir>")
    sys.exit(1)
else :
    input_paths = sys.argv[1:-1] 
    output_path = sys.argv[-1]

lines_df = spark.read.option("header", True).option("inferSchema", False).csv(input_paths)

lines_df = lines_df.select(
    *[F.col(c).cast("string") for c in lines_df.columns[:5]],
    *[F.col(c).cast("double") for c in lines_df.columns[5:]]
)
lines_df = lines_df.cache()

# Largest execution time jobs
timestamp_columns = [col for col in lines_df.columns if col.endswith("_TIMESTAMP")]
# Set -1 values to null in timestamp columns
for col in timestamp_columns:
    lines_df = lines_df.withColumn(col, F.when(F.col(col) == -1, None).otherwise(F.col(col)))
# Avoid unexpected behaviours 
valid_starts = [
    F.coalesce(F.col(c), F.lit(float("inf")))
    for c in timestamp_columns
]
valid_ends = [
    F.coalesce(F.col(c), F.lit(0))
    for c in timestamp_columns
]
# Compute minimum and maximum of all TIMESTAMP columns
min_timestamp = F.least(*valid_starts)
max_timestamp = F.greatest(*valid_ends)
job_times_df = lines_df.withColumn("min_timestamp", min_timestamp).withColumn("max_timestamp", max_timestamp)
job_times_df = job_times_df.withColumn("execution_time", F.col("max_timestamp") - F.col("min_timestamp"))
job_execution_times_df = job_times_df.groupBy("jobid").agg(
    F.min("min_timestamp").alias("job_start"), 
    F.max("max_timestamp").alias("job_end")
)
# No valid timestamps were found so we set to 0
job_execution_times_df = job_execution_times_df.withColumn(
    "execution_time", 
    F.when(F.col("job_start") == float("inf"), 0.0)
    .otherwise(F.col("job_end") - F.col("job_start"))
)
job_execution_times_df = job_execution_times_df.cache() # Will be reused
# Count 10%
total_jobs = job_execution_times_df.count()
top_n = ceil(total_jobs * 0.1)
longest_jobs_df = job_execution_times_df.orderBy(F.desc("execution_time")).limit(top_n)


# Largest I/O jobs
# We only consider files with fstype field set as "lustre"
lustre_df = lines_df.filter(lines_df.fstype == "lustre")
# Compute amount of data accessed
file_data_df = lustre_df.withColumn("data_amount", F.col("POSIX_BYTES_READ") + F.col("POSIX_BYTES_WRITTEN"))
file_data_df = file_data_df.cache()
# Regroup by jobid and sum data_amount
job_data_df = file_data_df.groupBy("jobid").agg(F.sum("data_amount").alias("total_data_amount"))
job_data_df = job_data_df.cache() # Will be reused
# Get top 10% jobs by data amount
largest_jobs_IO_df = job_data_df.orderBy(F.desc("total_data_amount")).limit(top_n)


# Users with largest I/O
user_data_df = file_data_df.groupBy("uid").agg(F.sum("data_amount").alias("total_data_amount"))
# Count all distinct users
total_users = lines_df.select("uid").distinct().count()
top_n_users = ceil(total_users * 0.1)
# Get uses with the largest data access
largest_users_IO_df = user_data_df.orderBy(F.desc("total_data_amount")).limit(top_n_users)
# Get all jobs from these users
largest_users_jobs_df = lines_df.join(F.broadcast(largest_users_IO_df), on="uid", how="inner").select("jobid").distinct()

# Gather all jobids from the three previous results
longest_jobids_df = longest_jobs_df.select("jobid")
largest_jobs_IO_jobids_df = largest_jobs_IO_df.select("jobid")
largest_users_jobids_df = largest_users_jobs_df.select("jobid")
jobids_df = longest_jobids_df.union(largest_jobs_IO_jobids_df).union(largest_users_jobids_df).distinct()

# Keep only the relevant jobs
filtered_line_df = lines_df.join(F.broadcast(jobids_df), on="jobid", how="inner")

# Basic information of the jobs (jobid, uid, execution time, total data accessed)
job_base_df = jobids_df \
    .join(job_execution_times_df, on="jobid", how="left") \
    .join(job_data_df, on="jobid", how="left")
    
uid_df = filtered_line_df.select("jobid", "uid").distinct()
job_base_df = job_base_df.join(uid_df, on="jobid", how="left")

job_base_df = job_base_df.withColumn(
    "total_data_amount",
    F.coalesce(F.col("total_data_amount"), F.lit(0))
)

# Keep only Lustre accesses with actual data transfer (>0 bytes)
lustre_events_df = filtered_line_df.filter(
    (F.col("fstype") == "lustre") &
    ((F.col("POSIX_BYTES_READ") + F.col("POSIX_BYTES_WRITTEN")) > 0)
)

# Calculate I/O time for each file access (coalesce skips null)
lustre_events_df = lustre_events_df.withColumn(
    "start_ts",
    F.least("POSIX_F_READ_START_TIMESTAMP", "POSIX_F_WRITE_START_TIMESTAMP")
).withColumn(
    "end_ts",
    F.greatest("POSIX_F_READ_END_TIMESTAMP", "POSIX_F_WRITE_END_TIMESTAMP")
).withColumn(
    "io_time",
    F.coalesce(F.col("POSIX_F_READ_TIME"), F.lit(0.0)) +
    F.coalesce(F.col("POSIX_F_WRITE_TIME"), F.lit(0.0))
)

# Filter out rows with invalid or missing timestamps
lustre_events_df = lustre_events_df.filter(
    F.col("start_ts").isNotNull() & 
    F.col("end_ts").isNotNull() & 
    (F.col("start_ts") <= F.col("end_ts"))
)

# Compute phases
# To merge overlapping file accesses, we sort by start time and track the maximum end time seen so far.
w_sort = Window.partitionBy("jobid").orderBy("start_ts", F.desc("end_ts"))

# Calculate 'prev_max_end' (max of end_ts in all previous sorted rows)
# rowsBetween(Window.unboundedPreceding, -1) corresponds to all rows before the current row
lustre_events_df = lustre_events_df.withColumn(
    "prev_max_end",
    F.max("end_ts").over(w_sort.rowsBetween(Window.unboundedPreceding, -1))
)

# A new phase starts if current start > max end of previous rows.
# (or if it's the first row: prev_max_end is null)
lustre_events_df = lustre_events_df.withColumn(
    "is_new_phase",
    F.when(
        (F.col("prev_max_end").isNull()) | 
        (F.col("start_ts") > F.col("prev_max_end")), 
        1
    ).otherwise(0)
)

# Assign a unique phase_id per job by summing 'is_new_phase'
lustre_events_df = lustre_events_df.withColumn(
    "phase_id",
    F.sum("is_new_phase").over(w_sort.rowsBetween(Window.unboundedPreceding, Window.currentRow))
)

# Calculate I/O time per phase (max of io_time in that phase)
phase_metrics_df = lustre_events_df.groupBy("jobid", "phase_id").agg(
    F.max("io_time").alias("phase_io_time")
)

# Calculate job total I/O time and total number of phases
job_io_metrics_df = phase_metrics_df.groupBy("jobid").agg(
    F.sum("phase_io_time").alias("job_io_time"),
    F.count("phase_id").alias("total_io_phases")
)

# Join the I/O metrics to the final job dataframe
final_df = job_base_df.join(job_io_metrics_df, on="jobid", how="left")

# If a job is not found in job_io_metrics_df (null), it means it had 0 I/O.
final_df = final_df.withColumn("job_io_time", F.coalesce(F.col("job_io_time"), F.lit(0.0)))
final_df = final_df.withColumn("total_io_phases", F.coalesce(F.col("total_io_phases"), F.lit(0)))

# Set to -1 if: execution time is missing or
# I/O time is strictly greater than execution time
final_df = final_df.withColumn(
    "job_io_time",
    F.when(
        (F.col("execution_time").isNull()) |
        (F.col("job_io_time") > F.col("execution_time")),
        -1.0
    ).otherwise(F.col("job_io_time"))
)

# Calculate total files accessed
job_files_df = filtered_line_df.filter(
    (F.col("fstype") == "lustre") &
    ((F.col("POSIX_BYTES_READ") + F.col("POSIX_BYTES_WRITTEN")) > 0)
).select("jobid", "recordid").distinct().groupBy("jobid").agg(
    F.count("recordid").alias("total_files")
)

# Add final columns to dataframe
final_df = final_df.join(job_files_df, on="jobid", how="left")
final_df = final_df.withColumn("total_files", F.coalesce(F.col("total_files"), F.lit(0)))

# Final Selection
final_df = final_df.select(
    "jobid",
    "uid",
    "execution_time",
    "total_data_amount",
    "job_io_time",
    "total_files",
    "total_io_phases"
).distinct().sort("jobid")

final_df.show()

end = timer()
print("Time : " + str(end - start))

df_result = spark.read.option("header", "false").option("inferSchema", "true").csv("/user/fzanonboito/CISD/topjobs/topjobs_1_to_3/*")
df_result = df_result.select(
    F.col("_c0").alias("jobid"),
    F.col("_c1").alias("uid"),
    F.col("_c2").alias("execution_time"),
    F.col("_c3").alias("total_data_amount"),
    F.col("_c4").alias("job_io_time"),
    F.col("_c5").alias("total_files"),
    F.col("_c6").alias("total_io_phases")
)

cols = [
    "execution_time",
    "total_data_amount",
    "job_io_time",
    "total_files",
    "total_io_phases"
]

for c in cols:
    final_df = final_df.withColumn(c, F.col(c).cast("double"))
    df_result = df_result.withColumn(c, F.col(c).cast("double"))


diff_final_minus_result = final_df.subtract(df_result)

print("Lignes présentes dans final_df mais absentes de df_result :",
      diff_final_minus_result.count())

diff_final_minus_result.sort("jobid").show(truncate=False)


diff_result_minus_final = df_result.subtract(final_df)

print("Lignes présentes dans df_result mais absentes de final_df :",
      diff_result_minus_final.count())

diff_result_minus_final.sort("jobid").show(truncate=False)

common_jobids = final_df.join(
    df_result,
    final_df.jobid == df_result.jobid,
    "inner"
).select(final_df.jobid).distinct()

print("common_jobids has " + str(common_jobids.count()))
common_jobids.show()


missing_in_jobids = df_result.join(
    final_df,
    df_result.jobid == final_df.jobid,
    "left_anti"
)

print("missing_in_jobids has " + str(missing_in_jobids.count()))
missing_in_jobids.show()

if len(sys.argv) != 1:
    jobids_df.write.mode("overwrite").csv(output_path)
