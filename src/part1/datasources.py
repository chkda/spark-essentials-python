import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType

base_path = os.path.join(os.path.dirname(__file__), "src", "main", "resources", "data")
cars_json_path = os.path.join(base_path, "cars.json")
stocks_csv_path = os.path.join(base_path, "stocks.csv")
text_file_path = os.path.join(base_path, "sampleTextFile.txt")
movies_json_path = os.path.join(base_path, "movies.json")


def main():
    spark = (SparkSession
             .builder
             .appName("Data Sources and Formats in Pyspark")
             .config("spark.master", "local")
             .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
             .getOrCreate())

    cars_schema = StructType(
        [
            StructField("Name", StringType()),
            StructField("Miles_per_Gallon", DoubleType()),
            StructField("Cylinders", LongType()),
            StructField("Displacement", DoubleType()),
            StructField("Horsepower", LongType()),
            StructField("Weight_in_lbs", LongType()),
            StructField("Acceleration", DoubleType()),
            StructField("Year", DateType()),
            StructField("Origin", StringType()),
        ]
    )

    cars_df = (spark.read.format("json")
               .schema(cars_schema)
               .option("mode", "failFast")
               .load("resources/data/cars.json"))

    cars_df_with_option_map = (spark.read.format("json")
                               .options(
        **{"mode": "failFast", "path": "resources/data/cars.json", "inferSchema": "true"})
                               .load())

    cars_df.write.format("json").mode("overwrite").save("resources/data/cars_dupe.json")
    cars_with_more_options_df = (
        spark.read.schema(cars_schema)
        .option("dateFormat", "yyyy-MM-dd")
        .option("allowSingleQuotes", "true")
        .option("compression", "uncompressed")
        .json("resources/data/cars.json")
    )

    stocks_schema = StructType(
        [
            StructField("symbol", StringType()),
            StructField("date", DateType()),
            StructField("price", DoubleType()),
        ]
    )

    stocks_df = (
        spark.read.schema(stocks_schema)
        .option("dateFormat", "MMM d yyyy")
        .option("header", "true")
        .option("sep", ",")
        .option("nullValue", "")
        .csv("resources/data/stocks.csv")
    )

    cars_df.write.mode("overwrite").save("resources/data/cars.parquet")

    spark.read.text("resources/data/sampleTextFile.txt").show()

    employees_df = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
        .option("user", "docker")
        .option("password", "docker")
        .option("dbtable", "public.employees")
        .load()
    )

    movies_df = (
        spark.read.format("json")
        .options(**{"mode": "failFast", "inferSchema": "true", "path": "resources/data/movies.json"})
        .load()
    )

    movies_df.show()

    (movies_df.write
     .mode("overwrite")
     .option("compression", "snappy")
     .save("resources/data/movies.parquet"))

    (movies_df.write
     .format("jdbc")
     .mode("overwrite")
     .option("driver", "org.postgresql.Driver")
     .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
     .option("user", "docker")
     .option("password", "docker")
     .option("dbtable", "public.movies")
     .save())


if __name__ == "__main__":
    main()
