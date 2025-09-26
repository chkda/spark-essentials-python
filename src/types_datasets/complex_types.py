import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main():
    spark = (SparkSession.builder
             .appName("Complex Types")
             .config("spark.master", "local")
             .config("spark.sql.ansi.enabled", "false")
             .getOrCreate())

    movies_df = (spark.read.format("json")
                 .option("inferSchema", "true")
                 .load("resources/data/movies.json"))

    movies_with_release_dates = movies_df.select(
        F.col("Title"),
        F.col("Release_Date"),
        F.coalesce(
            F.to_date(F.col("Release_Date"), "d-MMM-yy"),
            F.to_date(F.col("Release_Date"), "dd-MMM-yy"),
            F.to_date(F.col("Release_Date"), "d-MMM-yyyy"),
            F.to_date(F.col("Release_Date"), "dd-MMM-yy"),
            F.to_date(F.col("Release_Date"), "yyyy-MM-dd"),
        ).alias("Actual_Release")
    )

    # movies_with_release_dates.show()

    movies_with_dates_info = (movies_with_release_dates
                              .withColumn("Today", F.current_date())
                              .withColumn("Right_Now", F.current_timestamp())
                              .withColumn("Movie_Age", F.datediff(F.col("Today"), F.col("Actual_Release")) / 365))

    movies_with_dates_info.show()

    failed_parsing = movies_with_release_dates.select("*").where(F.col("Actual_Release").isNull())
    print("Records with failed date parsing")
    failed_parsing.show(5)

    # stocks_df = (spark.read.format("csv")
    #              .option("inferSchema", "true")
    #              .option("header", "true")
    #              .load("resources/data/stocks.csv"))
    #
    # stocks_df_with_dates = stocks_df.withColumn(
    #     "actual_date",
    #     F.to_date(F.col("date"), "MMM dd yyyy")
    # )
    #
    # stocks_df_with_dates.show()

    # movies_with_profit_struct = movies_df.select(
    #     F.col("Title"),
    #     F.col("Profit").getField("`US_Gross`").alias("US_Profit")
    # )
    # movies_with_profit_struct.show()


if __name__ == "__main__":
    main()
