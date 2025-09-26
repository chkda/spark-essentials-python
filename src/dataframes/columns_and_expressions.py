import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main():
    spark = (SparkSession.builder
             .appName("Columns and Expressions")
             .config("spark.master", "local")
             .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
             .getOrCreate())

    cars_df = (spark.read
               .format("json")
               .options(**{"mode": "failFast", "inferSchema": "true", "path": "resources/data/cars.json"})
               .load())

    first_col = cars_df["Name"]

    car_names_df = cars_df.select(first_col)

    # car_names_df.show()

    cars_df.select(
        F.col("Name"),
        F.column("Acceleration"),
        F.expr("Origin")
    ).show()

    cars_df.select("Name", "Origin").show()

    weight_in_kg_expression = cars_df["Weight_in_lbs"] / 2.2

    cars_with_weight_df = cars_df.select(
        F.col("Name"),
        F.col("Weight_in_lbs"),
        weight_in_kg_expression.alias("Weight_in_kg"),
        F.expr("Weight_in_lbs / 2.2").alias("Weight_in_kg_2")
    )

    # cars_with_weight_df.show()

    cars_with_select_expr_df = cars_df.selectExpr("Name", "Weight_in_lbs", "Weight_in_lbs / 2.2 as Weight_in_kg")
    # cars_with_select_expr_df.show()

    cars_with_kg_df = cars_df.withColumn("Weight_in_kg", F.col("Weight_in_lbs") / 2.2)
    # cars_with_kg_df.show()

    cars_with_column_renamed_df = cars_df.withColumnRenamed("Weight_in_lbs", "Weight in pounds")

    # cars_with_column_renamed_df.show()

    cars_with_column_renamed_df.selectExpr("`Weight in pounds`").show()

    cars_without_some_columns_df = cars_with_column_renamed_df.drop("Displacement", "Cylinders")
    # cars_without_some_columns_df.show()

    european_cars_df = cars_df.filter(F.col("Origin") != "USA")
    # european_cars_df.show()

    european_cars_df2 = cars_df.where(F.col("Origin") != "USA")
    # european_cars_df2.show()

    american_cars_df = cars_df.where("Origin = 'USA'")
    # american_cars_df.show()

    american_powerful_cars_df = cars_df.filter(F.col("Origin") == "USA").filter(F.col("Horsepower") > 150)
    # american_powerful_cars_df.show()

    american_powerful_cars_df2 = cars_df.filter((F.col("Origin") == "USA") & (F.col("Horsepower") > 150))
    # american_powerful_cars_df2.show()

    american_powerful_cars_df3 = cars_df.filter("Origin = 'USA' and Horsepower > 150")
    # american_powerful_cars_df3.show()

    more_cars_df = (spark.read.format("json")
                    .option("inferSchema", "true")
                    .load("resources/data/more_cars.json"))

    all_cars_df = cars_df.union(more_cars_df)
    # all_cars_df.show()

    all_countries = cars_df.select("Origin").distinct()
    all_countries.show()

    movies_df = (spark.read
                 .format("json")
                 .option("inferSchema", "true")
                 .load("resources/data/movies.json"))

    movies_df.select("Title", "US_Gross").show()

    movies_df.select(movies_df["Title"], F.col("US_Gross"), F.column("Worldwide_Gross")).show()

    movies_profit_df = movies_df.withColumn("Profit",
                                            F.coalesce(F.col("US_Gross"), F.lit(0)) +
                                            F.coalesce(F.col("Worldwide_Gross"), F.lit(0)) +
                                            F.coalesce(F.col("US_DVD_Sales"), F.lit(0)))

    # movies_profit_df.show()

    movies_profit_df2 = movies_df.selectExpr(
        "Title",
        "US_Gross",
        "Worldwide_Gross",
        "US_DVD_Sales",
        "COALESCE(US_Gross,0) + COALESCE(Worldwide_Gross,0) + COALESCE(US_DVD_Sales,0) as Total_Profit"
    )

    movies_profit_df2.show()

    good_comedies_df = movies_df.filter(F.col("Major_Genre") == "Comedy").filter(F.col("IMDB_rating") > 6)

    # good_comedies_df.show()

    good_comedies_df2 = movies_df.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
    # good_comedies_df2.show()

    good_comedies_df3 = movies_df.where((F.col("Major_Genre") == "Comedy") & (F.col("IMDB_rating") > 6))
    # good_comedies_df3.show()


if __name__ == "__main__":
    main()
