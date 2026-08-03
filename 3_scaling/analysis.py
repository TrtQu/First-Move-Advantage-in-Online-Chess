"""
First-Move Advantage in Online Chess -- Phase 3 (PySpark on Dataproc)

Question: Does White's first-move advantage change with player skill (Elo)
and with time control (bullet/blitz/rapid/classical)?

Run the same way regardless of cluster size -- the scaling experiment (1, 2,
4 workers) only changes the cluster, not this script. See scaling.txt for
the gcloud commands and recorded runtimes.
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, round as spark_round, sum as spark_sum, count, row_number, desc, substring_index

spark = SparkSession.builder.appName("FirstMoveAdvantage").getOrCreate()

start_time = time.time()

# 1. Read the raw PGN directly from GCS
# .zst is not a splittable codec, so spark.read.text() on a single .zst file
# always yields exactly ONE partition: the decompression itself is
# unavoidably a single-task/single-core step no matter how large the
# cluster is. Everything downstream of that read does NOT need to stay on
# one core, though -- repartition() shuffles the records out across the
# cluster before the CPU-heavy PGN parsing and the aggregations run, which
# is what actually lets 2- and 4-worker clusters outrun the 1-worker
# baseline in the scaling experiment.
#
# The record delimiter is set to a blank line ("\n\n") instead of the
# default single newline, so each row Spark reads is already one complete,
# self-contained PGN block (either the full multi-line tag header for one
# game, or its movetext) rather than one physical line. That removes any
# need to track state across rows to reassemble a game, which is what makes
# it safe to repartition (shuffle) afterward -- a stateful "carry the
# previous line's partial game" parser would silently produce wrong results
# once rows are redistributed out of file order.
spark.sparkContext.setLogLevel("WARN")

gcs_path = "gs://promising-cairn-501617-g0-cs131-lichess/lichess_db_standard_rated_2026-06.pgn.zst"
df_raw = spark.read.option("lineSep", "\n\n").text(gcs_path)
tag_blocks = df_raw.filter(col("value").startswith("[Event ")).repartition(200)

# 2. Explicit schema for the parsed, one-row-per-game table
game_schema = StructType([
    StructField("white_elo", IntegerType(), True),
    StructField("black_elo", IntegerType(), True),
    StructField("result", StringType(), True),
    StructField("time_control", StringType(), True),
    StructField("opening", StringType(), True),
])


def parse_tag_blocks(iterator):
    """Turn one game's whole tag-header block (one row) into one row tuple.

    Each input row is already a complete, self-contained record (see the
    record-delimiter note above), so this has no state to carry between
    rows -- safe to run on any row in any order, on any partition.

    Plain tuples (not pyspark.sql.Row(**kwargs)) are used deliberately: Row
    built from keyword arguments sorts its fields alphabetically by default,
    which would silently misalign values against the explicit schema below.
    A tuple's order always matches the order it's written in.
    """
    for row in iterator:
        game = {}
        for line in row.value.split("\n"):
            if line.startswith("[WhiteElo "):
                try:
                    game["white_elo"] = int(line.split('"')[1])
                except (IndexError, ValueError):
                    pass
            elif line.startswith("[BlackElo "):
                try:
                    game["black_elo"] = int(line.split('"')[1])
                except (IndexError, ValueError):
                    pass
            elif line.startswith("[Result "):
                game["result"] = line.split('"')[1]
            elif line.startswith("[TimeControl "):
                game["time_control"] = line.split('"')[1]
            elif line.startswith("[Opening "):
                game["opening"] = line.split('"')[1]

        if "white_elo" in game and "result" in game:
            yield (
                game.get("white_elo"),
                game.get("black_elo"),
                game.get("result"),
                game.get("time_control"),
                game.get("opening"),
            )


parsed_rdd = tag_blocks.rdd.mapPartitions(parse_tag_blocks)
games = spark.createDataFrame(parsed_rdd, schema=game_schema)

# 3. Transform: Elo bracket, time-control category, win-flag columns
def base_seconds(tc_col):
    # "180+0" -> 180 ; malformed/"-" values fall through to null
    return when(tc_col.contains("+"), substring_index(tc_col, "+", 1).cast("int"))


games = games.filter(col("result").isin("1-0", "0-1", "1/2-1/2")) \
    .withColumn("elo_bracket", (col("white_elo") / 100).cast("int") * 100) \
    .withColumn("base_seconds", base_seconds(col("time_control"))) \
    .withColumn(
        "time_category",
        when(col("base_seconds") < 180, "Bullet")
        .when(col("base_seconds") < 600, "Blitz")
        .when(col("base_seconds") < 1800, "Rapid")
        .when(col("base_seconds").isNotNull(), "Classical")
        .otherwise("Unknown"),
    ) \
    .withColumn("white_win", when(col("result") == "1-0", 1).otherwise(0)) \
    .withColumn("black_win", when(col("result") == "0-1", 1).otherwise(0)) \
    .withColumn("draw", when(col("result") == "1/2-1/2", 1).otherwise(0))

# Cached: this DataFrame feeds three separate actions below (elo-bracket
# stats, the elo-tier join, and the windowed top-openings query), so caching
# avoids re-running the PGN parse three times.
games.cache()

# 4. groupBy: White/Black win % and draw % per Elo bracket
elo_stats = games.groupBy("elo_bracket").agg(
    spark_sum("white_win").alias("white_wins"),
    spark_sum("black_win").alias("black_wins"),
    spark_sum("draw").alias("draws"),
    count("*").alias("total_games"),
).withColumn("white_win_pct", spark_round((col("white_wins") / col("total_games")) * 100, 2)) \
 .withColumn("black_win_pct", spark_round((col("black_wins") / col("total_games")) * 100, 2)) \
 .withColumn("draw_pct", spark_round((col("draws") / col("total_games")) * 100, 2)) \
 .filter(col("total_games") > 1000) \
 .orderBy("elo_bracket")

print("=== White-advantage by Elo bracket ===")
elo_stats.show(30, truncate=False)

# 5. Join: label each Elo bracket with a skill tier from a small reference
#  DataFrame (range join on elo_bracket BETWEEN tier_min AND tier_max)
tier_rows = [
    (0, 800, "Beginner"),
    (800, 1200, "Novice"),
    (1200, 1600, "Intermediate"),
    (1600, 2000, "Advanced"),
    (2000, 2400, "Expert"),
    (2400, 9999, "Master"),
]
tier_schema = StructType([
    StructField("tier_min", IntegerType(), False),
    StructField("tier_max", IntegerType(), False),
    StructField("tier_label", StringType(), False),
])
tiers = spark.createDataFrame(tier_rows, schema=tier_schema)

elo_stats_with_tier = elo_stats.join(
    tiers,
    (col("elo_bracket") >= tiers.tier_min) & (col("elo_bracket") < tiers.tier_max),
    "left",
).select("elo_bracket", "tier_label", "white_win_pct", "black_win_pct", "draw_pct", "total_games") \
 .orderBy("elo_bracket")

print("=== White-advantage by Elo bracket, joined with skill-tier labels ===")
elo_stats_with_tier.show(30, truncate=False)

# 6. Window function: top-3 openings per time-control category, ranked by how often White wins with them
opening_stats = games.filter(col("opening").isNotNull()).groupBy("time_category", "opening").agg(
    count("*").alias("games_played"),
    spark_sum("white_win").alias("white_wins"),
).filter(col("games_played") > 500) \
 .withColumn("white_win_pct", spark_round((col("white_wins") / col("games_played")) * 100, 2))

rank_window = Window.partitionBy("time_category").orderBy(desc("white_win_pct"))
top_openings = opening_stats.withColumn("rank", row_number().over(rank_window)) \
    .filter(col("rank") <= 3) \
    .orderBy("time_category", "rank")

print("=== Top 3 openings for White, by time-control category ===")
top_openings.show(30, truncate=False)

# 7. Spark SQL: same white-advantage question, expressed declaratively
games.createOrReplaceTempView("games")
sql_stats = spark.sql("""
    SELECT
        time_category,
        COUNT(*) AS total_games,
        ROUND(100.0 * SUM(CASE WHEN result = '1-0' THEN 1 ELSE 0 END) / COUNT(*), 2) AS white_win_pct,
        ROUND(100.0 * SUM(CASE WHEN result = '1/2-1/2' THEN 1 ELSE 0 END) / COUNT(*), 2) AS draw_pct
    FROM games
    GROUP BY time_category
    ORDER BY total_games DESC
""")

print("=== White-advantage by time-control category (Spark SQL) ===")
sql_stats.show(truncate=False)

games.unpersist()

end_time = time.time()
print(f"--- Scaling Experiment Runtime: {end_time - start_time:.2f} seconds ---")

spark.stop()
