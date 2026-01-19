# TODO : create a RDD with all the jobs from the 10% users that access the largest amount of data.

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from timeit import default_timer as timer
from pyspark.sql.window import Window
from math import ceil
import os
import sys



os.environ["SPARK_LOCAL_DIRS"] = "/home/llahlah/spark_tmp"
os.environ["TMPDIR"] = "/home/llahlah/spark_tmp"

conf = SparkConf() \
    .setAppName("projet") \
    .set("spark.local.dir", "/home/llahlah/spark_tmp")

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


job_base_df = jobids_df \
    .join(job_execution_times_df, on="jobid", how="left") \
    .join(job_data_df, on="jobid", how="left")
    
uid_df = lines_df.select("jobid", "uid").distinct()
job_base_df = job_base_df.join(uid_df, on="jobid", how="left")

job_base_df = job_base_df.withColumn(
    "total_data_amount",
    F.coalesce(F.col("total_data_amount"), F.lit(0))
)

lustre_io_df = lines_df.filter(
    (F.col("fstype") == "lustre") &
    ((F.col("POSIX_BYTES_READ") + F.col("POSIX_BYTES_WRITTEN")) > 0)
).select(
    "jobid", "uid",
    "POSIX_F_READ_START_TIMESTAMP", "POSIX_F_READ_END_TIMESTAMP",
    "POSIX_F_WRITE_START_TIMESTAMP", "POSIX_F_WRITE_END_TIMESTAMP",
    "POSIX_F_READ_TIME", "POSIX_F_WRITE_TIME"
)

lustre_io_df = lustre_io_df.withColumn(
    "start_ts",
    F.least(
        F.coalesce(F.col("POSIX_F_READ_START_TIMESTAMP"), F.lit(float("inf"))),
        F.coalesce(F.col("POSIX_F_WRITE_START_TIMESTAMP"), F.lit(float("inf")))
    )
).withColumn(
    "end_ts",
    F.greatest(
        F.coalesce(F.col("POSIX_F_READ_END_TIMESTAMP"), F.lit(0)),
        F.coalesce(F.col("POSIX_F_WRITE_END_TIMESTAMP"), F.lit(0))
    )
).withColumn(
    "io_time",
    F.coalesce(F.col("POSIX_F_READ_TIME"), F.lit(0)) +
    F.coalesce(F.col("POSIX_F_WRITE_TIME"), F.lit(0))
)




# Fusion des phases I/O par job
w = Window.partitionBy("jobid").orderBy("start_ts")

lustre_io_df = lustre_io_df.withColumn("prev_end", F.lag("end_ts").over(w))

lustre_io_df = lustre_io_df.withColumn(
    "new_phase",
    F.when(
        (F.col("prev_end").isNull()) |
        (F.col("start_ts") > F.col("prev_end")),
        1
    ).otherwise(0)
)

lustre_io_df = lustre_io_df.withColumn(
    "phase_id",
    F.sum("new_phase").over(w)
)

io_phases_df = lustre_io_df.groupBy("jobid", "phase_id").agg(
    F.max("io_time").alias("phase_io_time")
)

job_io_df = io_phases_df.groupBy("jobid").agg(
    F.sum("phase_io_time").alias("job_io_time")
)

job_io_df = job_io_df.join(
    job_execution_times_df.select("jobid", "execution_time"),
    on="jobid",
    how="left"
)

job_io_df = job_io_df.withColumn(
    "job_io_time",
    F.when(
        (F.col("job_io_time") == 0) |
        (F.col("execution_time").isNull()) |
        (F.col("job_io_time") > F.col("execution_time")),
        -1
    ).otherwise(F.col("job_io_time"))
)

# Nettoyage : SEULEMENT ce qui sera joint
job_io_df = job_io_df.select("jobid", "job_io_time")

final_df = job_base_df.join(job_io_df, on="jobid", how="left")


final_df = final_df.withColumn(
    "job_io_time",
    F.when(
        (F.col("job_io_time").isNull()) |
        (F.col("job_io_time") == 0) |
        (F.col("job_io_time") > F.col("execution_time")),
        -1
    ).otherwise(F.col("job_io_time"))
)


# Calcul du nombre de fichier (accessed via POSIX interface from the Lustre file system)

job_files_df = lines_df.filter(
    (F.col("fstype") == "lustre") &
    ((F.col("POSIX_BYTES_READ") + F.col("POSIX_BYTES_WRITTEN")) > 0)
).select(
    "jobid",
    "recordid"
).distinct().groupBy("jobid").agg(
    F.count("recordid").alias("total_files")
)




# S'assurer que les jobs sans fichiers Lustre ont 0
final_df = final_df.join(job_files_df, on="jobid", how="left")
final_df = final_df.withColumn("total_files", F.coalesce(F.col("total_files"), F.lit(0)))



# Nombre total de phase I/O

job_phases_df = io_phases_df.select(
    "jobid", "phase_id"
).distinct().groupBy("jobid").agg(
    F.count("*").alias("total_io_phases")
)

final_df = final_df.join(job_phases_df, on="jobid", how="left")

# Remplacer les nulls par 0 si un job n'a aucune phase I/O
final_df = final_df.withColumn(
    "total_io_phases",
    F.coalesce(F.col("total_io_phases"), F.lit(0))
)


final_df = final_df.select(
    "jobid",
    "uid",
    "execution_time",
    "total_data_amount",
    "job_io_time",
    "total_files",
    "total_io_phases"
).distinct().sort("jobid")



# Compare with existing results
df_result = spark.read.option("header", "false").option("inferSchema", "true").csv("/user/fzanonboito/CISD/topjobs/topjobs_9/*")
print("df_result has " + str(df_result.count()))
df_result.sort("_c0").show()


print("final_df has " + str(final_df.count()))
final_df.sort("jobid").show()



df_result = df_result.withColumnRenamed("_c0", "jobid_result")

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
