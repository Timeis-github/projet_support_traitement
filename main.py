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
    lines_df = spark.read.load("/user/fzanonboito/CISD/darshan/9/Darshan.csv", format = "csv", header = "true", inferSchema = "true")
elif len(sys.argv) < 3:
    print("Usage: spark-submit script.py <csv1> <csv2> ... <output_dir>")
    sys.exit(1)
else :
    input_paths = sys.argv[1:-1] 
    output_path = sys.argv[-1]    
    dfs = [spark.read.option("header", True).option("inferSchema", True).csv(p) for p in input_paths]
    lines_df = dfs[0]
    for df in dfs[1:]:
        lines_df = lines_df.unionByName(df)


for i in range(5, len(lines_df.columns)):
    lines_df = lines_df.withColumn(lines_df.columns[i], lines_df[lines_df.columns[i]].cast("double"))
lines_df = lines_df.cache()

# Largest execution time jobs
# execution time is the difference between the minimum of all _TIMESTAMP fields and the maximum of all _TIMESTAMP fields of all rows with the same jobid
timestamp_columns = [col for col in lines_df.columns if col.endswith("_TIMESTAMP")]
# Set -1 values to null in timestamp columns
for col in timestamp_columns:
    lines_df = lines_df.withColumn(col, F.when(F.col(col) == -1, None).otherwise(F.col(col)))
valid_starts = [
    F.coalesce(F.col(c), F.lit(float("inf")))
    for c in timestamp_columns
]
valid_ends = [
    F.coalesce(F.col(c), F.lit(0))
    for c in timestamp_columns
]

min_timestamp = F.least(*valid_starts)
max_timestamp = F.greatest(*valid_ends)

job_times_df = lines_df.withColumn("min_timestamp", min_timestamp).withColumn("max_timestamp", max_timestamp)
job_times_df = job_times_df.withColumn("execution_time", F.col("max_timestamp") - F.col("min_timestamp"))
job_execution_times_df = job_times_df.groupBy("jobid").agg(
    F.min("min_timestamp").alias("job_start"), 
    F.max("max_timestamp").alias("job_end")
)

# FIX: Check if job_start is infinite (meaning no valid timestamps were found).
# If so, force execution_time to 0.0.
job_execution_times_df = job_execution_times_df.withColumn(
    "execution_time", 
    F.when(F.col("job_start") == float("inf"), 0.0)
    .otherwise(F.col("job_end") - F.col("job_start"))
)
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


job_base_df = jobids_df \
    .join(job_execution_times_df, on="jobid", how="left") \
    .join(job_data_df, on="jobid", how="left")
    
uid_df = lines_df.select("jobid", "uid").distinct()
job_base_df = job_base_df.join(uid_df, on="jobid", how="left")

job_base_df = job_base_df.withColumn(
    "total_data_amount",
    F.coalesce(F.col("total_data_amount"), F.lit(0))
)



# ==============================================================================
# CORRECTED LOGIC FOR I/O PHASES AND TIME
# ==============================================================================

# 1. Filter and Prepare Data
# Keep only Lustre accesses with actual data transfer (>0 bytes)
lustre_events_df = lines_df.filter(
    (F.col("fstype") == "lustre") &
    ((F.col("POSIX_BYTES_READ") + F.col("POSIX_BYTES_WRITTEN")) > 0)
)

# Calculate Start/End timestamps and IO Time for each file access
# We use F.least/greatest to handle cases where a file is only Read or only Written.
# These functions skip nulls, effectively taking the available valid timestamp.
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

# 2. Merge Overlapping Intervals (I/O Phases)
# To merge intervals, we sort by start time and track the maximum end time seen so far.
w_sort = Window.partitionBy("jobid").orderBy("start_ts", F.desc("end_ts"))

# Calculate 'prev_max_end': the max end_ts of all previous rows in the sorted window.
# We use rowsBetween(Window.unboundedPreceding, -1) to look at everything strictly before the current row.
lustre_events_df = lustre_events_df.withColumn(
    "prev_max_end",
    F.max("end_ts").over(w_sort.rowsBetween(Window.unboundedPreceding, -1))
)

# A new phase starts if there is a gap: current start > max end of previous rows.
# (Or if it's the very first row, i.e., prev_max_end is null)
lustre_events_df = lustre_events_df.withColumn(
    "is_new_phase",
    F.when(
        (F.col("prev_max_end").isNull()) | 
        (F.col("start_ts") > F.col("prev_max_end")), 
        1
    ).otherwise(0)
)

# Assign a unique phase_id per job by cumulatively summing 'is_new_phase'
lustre_events_df = lustre_events_df.withColumn(
    "phase_id",
    F.sum("is_new_phase").over(w_sort.rowsBetween(Window.unboundedPreceding, Window.currentRow))
)

# 3. Aggregation
# Calculate IO Time per Phase: Max of file_io_time within that phase
phase_metrics_df = lustre_events_df.groupBy("jobid", "phase_id").agg(
    F.max("io_time").alias("phase_io_time")
)

# Calculate Job Total IO Time and Total Phases
job_io_metrics_df = phase_metrics_df.groupBy("jobid").agg(
    F.sum("phase_io_time").alias("job_io_time"),
    F.count("phase_id").alias("total_io_phases")
)

# 4. Join and Clean Results (CORRECTED)
# Join the I/O metrics back to the main job list
# We use a Left Join because jobs without Lustre I/O won't exist in job_io_metrics_df
final_df = job_base_df.join(job_io_metrics_df, on="jobid", how="left")

# STEP A: Handle "No I/O" cases first
# If a job is not found in job_io_metrics_df (NULL), it means it had 0 I/O.
final_df = final_df.withColumn("job_io_time", F.coalesce(F.col("job_io_time"), F.lit(0.0)))
final_df = final_df.withColumn("total_io_phases", F.coalesce(F.col("total_io_phases"), F.lit(0)))

# STEP B: Apply Validation Logic
# Set to -1 ONLY if:
# 1. Execution time is missing (cannot validate)
# 2. I/O Time is strictly greater than Execution Time (impossible data)
final_df = final_df.withColumn(
    "job_io_time",
    F.when(
        (F.col("execution_time").isNull()) |
        (F.col("job_io_time") > F.col("execution_time")),
        -1.0
    ).otherwise(F.col("job_io_time"))
)

# 5. Calculate Total Files
job_files_df = lines_df.filter(
    (F.col("fstype") == "lustre") &
    ((F.col("POSIX_BYTES_READ") + F.col("POSIX_BYTES_WRITTEN")) > 0)
).select("jobid", "recordid").distinct().groupBy("jobid").agg(
    F.count("recordid").alias("total_files")
)

# 6. Final Assembly with File Counts
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



df_result = spark.read.option("header", "false").option("inferSchema", "true").csv("/user/fzanonboito/CISD/topjobs/topjobs_9/*")
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

# common_jobids = final_df.join(
#     df_result,
#     final_df.jobid == df_result.jobid_result,
#     "inner"
# ).select(final_df.jobid).distinct()

# print("common_jobids has " + str(common_jobids.count()))
# common_jobids.show()


# missing_in_jobids = df_result.join(
#     final_df,
#     df_result.jobid_result == final_df.jobid,
#     "left_anti"
# )

# print("missing_in_jobids has " + str(missing_in_jobids.count()))
# missing_in_jobids.show()

if len(sys.argv) != 1:
    jobids_df.write.mode("overwrite").csv(output_path)

end = timer()
print("Time : " + str(end - start))
