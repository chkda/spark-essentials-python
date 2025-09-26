import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main():
    spark = (SparkSession.builder
             .appName("Aggregations and Grouping")
             .config("spark.master", "local")
             .getOrCreate())

    movies_df = (spark.read
                 .format("json")
                 .option("inferSchema", "true")
                 .load("resources/data/movies.json"))

    genres_count_df = movies_df.select(F.count("Major_Genre"))
    # genres_count_df.show()

    movies_df.selectExpr("count(Major_Genre)").show()

    movies_df.select(F.count("*")).show()

    movies_df.select(F.countDistinct("Major_Genre")).show()

    movies_df.select(F.approx_count_distinct("Major_Genre")).show()

    min_rating_df = movies_df.select(F.min("IMDB_Rating"))
    # min_rating_df.show()

    movies_df.selectExpr("min(IMDB_Rating)").show()

    sum_rating_df = movies_df.select(F.sum("US_Gross"))
    # sum_rating_df.show()

    movies_df.selectExpr("sum(US_Gross)").show()

    avg_rating_df = movies_df.select(F.avg("Rotten_Tomatoes_Rating"))
    # avg_rating_df.show()

    movies_df.selectExpr("avg(Rotten_Tomatoes_Rating)").show()

    movies_df.select(
        F.mean("Rotten_Tomatoes_Rating"),
        F.stddev("Rotten_Tomatoes_Rating")
    ).show()

    count_by_genre_df = movies_df.groupBy("Major_Genre").count()
    count_by_genre_df.show()

    avg_rating_by_genre_df = movies_df.groupBy("Major_Genre").avg("IMDB_Rating")
    avg_rating_by_genre_df.show()

    aggregation_by_genre_df = (movies_df.groupBy("Major_Genre")
                               .agg(
        F.count("*").alias("N_Movies"),
        F.avg("IMDB_Rating").alias("Avg_Rating")
    ).orderBy(F.col("Avg_Rating")))
    aggregation_by_genre_df.show()

    print("Total Profit")
    (
        movies_df.select(
            (F.coalesce(F.col("US_Gross"), F.lit(0)) +
             F.coalesce(F.col("US_DVD_Sales"), F.lit(0)) +
             F.coalesce(F.col("Worldwide_Gross"), F.lit(0))).alias("Profits")
        ).select(F.sum("Profits"))
    ).show()

    print("\n Distinct Directors")
    movies_df.select(F.countDistinct("Director")).show()

    print("\n Mean and stddev for US Gross revenue")
    movies_df.select(F.mean("US_Gross"), F.stddev("US_Gross")).show()

    print("\n Avg Rating and Total US Gross per director")
    (movies_df.groupBy("Director")
     .agg(
        F.avg("IMDB_Rating").alias("Avg_Rating"),
        F.sum("US_Gross").alias("Total_Gross"),
    ).orderBy(F.col("Avg_Rating").desc_nulls_last())).show()


if __name__ == "__main__":
    main()
