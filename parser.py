from pyspark.sql import SparkSession

# Start a Spark Session
spark = SparkSession.builder \
    .appName("TXT to CSV Converter") \
    .getOrCreate()

# Define the path pattern to read all .txt files within nested folders
folder_path = "/path to the data folder/data/Normal Crawl/*/*/*.txt"

# Read all .txt files matching the path pattern
df = spark.read.option("delimiter", "\t").csv(folder_path, header=False, inferSchema=True)

# Assign Column Names only for the first 10 columns
columns = ["video_ID", "uploader", "age", "category", "length", "views", "rate", "ratings", "comments", "related_IDs"]
num_cols = len(df.columns)
if num_cols >= 10:
    for i in range(10):
        df = df.withColumnRenamed(df.columns[i], columns[i])

# Define the output path
output_path = "/path/to/output/"

# Write to .csv files, one file per input .txt file
df.write.csv(output_path, header=True, mode="overwrite")

# Stop the Spark Session
spark.stop()
