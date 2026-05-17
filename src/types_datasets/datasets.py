from dataclasses import dataclass
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import SparkSession


@dataclass
class Car:
    name: str
    miles_per_gallon: Optional[float]
    cylinders: int
    displacement: float
    horsepower: Optional[int]
    weight_in_lbs: int
    acceleration: float
    year: str
    origin: str


spark = (SparkSession.builder.
         appName("Datasets").
         config("spark.master", "local").
         getOrCreate())


def read_df(filename: str):
    return (spark.read.
            option("inferSchema", "true").
            json(f"resources/data/{filename}"))


def main():
    numbers_df = (spark.read.
                  format("csv").
                  option("header", "true").
                  option("inferSchema", "true").
                  load("resources/data/numbers.csv"))

    # numbers_df.printSchema()
    # numbers_df.show()

    small_numbers_df = numbers_df.filter(F.col("numbers") < 100)
    # small_numbers_df.show()

    cars_df = read_df("cars.json")
    cars_df.printSchema()
    cars_df.show()

    first_car_row = cars_df.first()
    first_car = Car(
        name=first_car_row["Name"],
        miles_per_gallon=first_car_row["Miles_per_Gallon"],
        cylinders=first_car_row["Cylinders"],
        displacement=first_car_row["Displacement"],
        horsepower=first_car_row["Horsepower"],
        weight_in_lbs=first_car_row["Weight_in_lbs"],
        acceleration=first_car_row["Acceleration"],
        year=first_car_row["Year"],
        origin=first_car_row["Origin"],
    )

    print(first_car.name)
    print(first_car.miles_per_gallon)

    car_names_df = cars_df.select(F.upper(F.col("Name")).alias("Name"))
    car_names_df.show()

    cars_count = cars_df.count()
    print(f"Cars count: {cars_count}")

    powerful_cars_count = (cars_df.
                           filter(F.coalesce(F.col("Horsepower"), F.lit(0)) > 140).
                           count())

    print(f"Powerful cars count: {powerful_cars_count}")

    cars_grouped_by_origin_df = (
        cars_df.groupBy("Origin").count()
        # Wide transformation - JOINS and GROUPBY - shuffle data, gather data from all partitions
    )

    cars_grouped_by_origin_df.show()
    cars_grouped_by_origin_df.explain()


if __name__ == "__main__":
    main()
