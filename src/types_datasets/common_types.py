import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main():
    spark = (SparkSession.builder
             .appName("Common Types")
             .config("spark.master", "local")
             .getOrCreate())

    movies_df = (spark.read.format("json")
                 .option("inferSchema", "true")
                 .load("resources/data/movies.json"))

    movies_df.select(F.col("Title"), F.lit(47).alias("plain_value"))

    drama_filter = F.col("Major_Genre") == "Drama"
    good_rating_filter = F.col("IMDB_Rating") > 7.0
    preferred_filter = drama_filter & good_rating_filter

    movies_df.select("Title").where(drama_filter).show()

    movies_with_goodness_flags_df = movies_df.select(
        F.col("Title"),
        preferred_filter.alias("good_movie")
    )

    movies_with_goodness_flags_df.where(~(F.col("good_movie"))).show()

    movies_avg_ratings_df = movies_df.select(
        F.col("Title"),
        ((F.col("Rotten_Tomatoes_Rating") / 10 + F.col("IMDB_Rating")) / 2).alias("avg_rating")
    )

    movies_avg_ratings_df.show()

    correlation = movies_df.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")
    print(f"Correlation between RT and IMDB ratings: {correlation}")

    cars_df = (spark.read.format("json")
               .option("inferSchema", "true")
               .load("resources/data/cars.json"))

    cars_df.select(F.initcap(F.col("Name"))).show()

    cars_df.select("*").where(F.col("Name").contains("volkswagen")).show()

    regex_string = "volkswagen|vw"

    vw_df = cars_df.select(
        F.col("Name"),
        F.regexp_extract(F.col("Name"), regex_string, 0).alias("regex_extract")
    ).where(F.col("regex_extract") != "").drop("regex_extract")

    vw_df.show()

    vw_df.select(
        F.col("Name"),
        F.regexp_replace(F.col("Name"), regex_string, "People's Car").alias("regex_replace")
    ).show()

    def get_car_names():
        return ["Volkswagen", "Mercedes-Benz", "Ford"]

    car_names = get_car_names()
    complex_regex = "|".join([name.lower() for name in car_names])
    print(f"Complex regex pattern: {complex_regex}")

    regex_filtered_cars = cars_df.select(
        F.col("Name"),
        F.regexp_extract(F.col("Name"), complex_regex, 0).alias("regex_extract")
    ).where(F.col("regex_extract") != "").drop("regex_extract")

    regex_filtered_cars.show()


if __name__ == "__main__":
    main()
