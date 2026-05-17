import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.
    appName("Managing Nulls").
    config("spark.master", "local").
    getOrCreate()
)


def main():
    movies_df = (
        spark.read
        .option("inferSchema", "true")
        .json("resources/data/movies.json")
    )

    movies_df.printSchema()

    movies_df.select(
        F.col("Title"),
        F.col("Rotten_Tomatoes_Rating"),
        F.col("IMDB_Rating"),
        F.coalesce(
            F.col("Rotten_Tomatoes_Rating"),
            F.col("IMDB_Rating") * 10,
        ).alias("Computed_Rating")
    ).show()

    movies_with_no_rt_rating_df = (
        movies_df
        .select("*")
        .where(F.col("Rotten_Tomatoes_Rating").isNull())
    )

    movies_with_no_rt_rating_df.show()

    movies_with_rt_rating_df = (
        movies_df
        .select("*")
        .where(F.col("Rotten_Tomatoes_Rating").isNotNull())
    )

    movies_with_rt_rating_df.show()

    movies_by_rating_df = movies_df.orderBy(
        F.col("Rotten_Tomatoes_Rating").desc_nulls_last()
    )
    movies_by_rating_df.select("Title", "Rotten_Tomatoes_Rating").show()

    movies_by_rating_df = movies_df.orderBy(
        F.col("Rotten_Tomatoes_Rating").desc_nulls_first()
    )
    movies_by_rating_df.select("Title", "Rotten_Tomatoes_Rating").show()

    movies_without_null_ratings_df = movies_df.na.drop(subset=["Rotten_Tomatoes_Rating", "IMDB_Rating"])
    movies_without_null_ratings_df.select("Title", "Rotten_Tomatoes_Rating", "IMDB_Rating").show()

    filled_movies_df = movies_df.na.fill({
        "IMDB_Rating": 0,
        "Rotten_Tomatoes_Rating": 10,
        "Director": "Unknown",
    })

    filled_movies_df.select("Title", "Rotten_Tomatoes_Rating", "IMDB_Rating", "Director").show()


if __name__ == "__main__":
    main()
