from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


def main():
    spark = (SparkSession
             .builder
             .appName("DataFrame Basics")
             .config("spark.master", "local")
             .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
             .getOrCreate())

    first_df = (spark.
                read.
                format("json")
                .option("inferSchema", "true")
                .load("resources/data/cars.json"))

    print("Dataframe content")
    first_df.show()
    print("Dataframe schema")
    first_df.printSchema()

    print("\n First 10 rows")
    for row in first_df.take(10):
        print(row)

    car_schema = StructType(
        [
            StructField("Name", StringType()),
            StructField("Miles_per_Gallon", DoubleType()),
            StructField("Cylinders", LongType()),
            StructField("Displacement", DoubleType()),
            StructField("Horsepower", LongType()),
            StructField("Weight_in_lbs", LongType()),
            StructField("Acceleration", DoubleType()),
            StructField("Year", StringType()),
            StructField("Origin", StringType()),
        ]
    )

    cars_df = (spark.
               read
               .format("json")
               .schema(car_schema)
               .option("mode", "failFast")
               .load("resources/data/cars.json"))

    cars_df.show()

    my_row = Row(
        "chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA"
    )

    cars = [
        ("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA"),
        ("buick skylark 320", 15, 8, 350, 165, 3693, 11.5, "1970-01-01", "USA"),
        ("plymouth satellite", 18, 8, 318, 150, 3436, 11.0, "1970-01-01", "USA"),
    ]

    column_names = ["Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "Origin"]
    manual_cars_df = spark.createDataFrame(cars, column_names)

    print("\n Manually created Dataframes")
    manual_cars_df.show()

    smartphones = [
        ("Samsung", "Galaxy S23", "Android", 50),
        ("Apple", "iPhone 15", "iOS", 48),
        ("Google", "Pixel 8", "Android", 50),
    ]

    smartphones_df = spark.createDataFrame(smartphones, ["Company", "Model", "OS", "CameraMP"])
    print("\n Smartphones dataframe")
    smartphones_df.show()

    movies_df = (
        spark
        .read
        .format("json")
        .option("inferSchema", "true")
        .load("resources/data/movies.json")
    )

    print("\n Movies Dataframe schema")
    movies_df.printSchema()

    n_rows = movies_df.count()
    print("\n Total rows:", n_rows)


if __name__ == "__main__":
    main()
