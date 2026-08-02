import time
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import col, when, round, sum, count

# Initialize Spark
spark = SparkSession.builder.appName("FirstMoveAdvantage").getOrCreate()

start_time = time.time()

# 1. Read the compressed file directly from GCS
gcs_path = "gs://tactical-coder-499516-s9-cs131demo/ws5/lichess.pgn.gz"
df_raw = spark.read.text(gcs_path)

# 2. RDD function to parse the multi-line PGN format into single rows
def parse_games(iterator):
    game = {}
    for row in iterator:
        line = row.value
        if line.startswith('[Event '):
            # If we finished reading a game, yield it to the DataFrame
            if 'elo' in game and 'result' in game:
                yield Row(elo=game['elo'], result=game['result'])
            game = {} # Reset for the next game
        elif line.startswith('[WhiteElo '):
            try:
                # Extract the number from [WhiteElo "1500"]
                game['elo'] = int(line.split('"')[1])
            except:
                pass
        elif line.startswith('[Result '):
            # Extract the result from [Result "1-0"]
            game['result'] = line.split('"')[1]
            
    # Yield the very last game
    if 'elo' in game and 'result' in game:
        yield Row(elo=game['elo'], result=game['result'])

# 3. Convert the parsed lines back into a clean DataFrame
parsed_rdd = df_raw.rdd.mapPartitions(parse_games)
df = spark.createDataFrame(parsed_rdd)

# 4. Transform: Create Elo brackets and calculate win flags
df = df.filter(col("result").isin("1-0", "0-1", "1/2-1/2")) \
       .withColumn("elo_bracket", (col("elo") / 100).cast("int") * 100) \
       .withColumn("white_win", when(col("result") == "1-0", 1).otherwise(0)) \
       .withColumn("black_win", when(col("result") == "0-1", 1).otherwise(0)) \
       .withColumn("draw", when(col("result") == "1/2-1/2", 1).otherwise(0))

# 5. Group by Elo bracket and calculate win percentages
stats = df.groupBy("elo_bracket").agg(
    sum("white_win").alias("white_wins"),
    sum("black_win").alias("black_wins"),
    sum("draw").alias("draws"),
    count("*").alias("total_games")
).withColumn(
    "white_win_pct", round((col("white_wins") / col("total_games")) * 100, 2)
).withColumn(
    "black_win_pct", round((col("black_wins") / col("total_games")) * 100, 2)
).filter(col("total_games") > 1000) \
 .orderBy("elo_bracket")

# 6. Show results and force execution
stats.show(30, truncate=False)

end_time = time.time()
print(f"--- Scaling Experiment Runtime: {end_time - start_time} seconds ---")

spark.stop()
