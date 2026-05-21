from typing import NamedTuple

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Introduction to RDDs")
    .config("spark.master", "local[*]")
    .getOrCreate()
)


class StockValue(NamedTuple):
    symbol: str
    date: str
    price: float


def read_stocks(filename: str) -> list[StockValue]:
    with open(filename, "r") as f:
        lines = f.readlines()

    stock_values = []
    for line in lines[1:]:
        tokens = line.strip().split(",")
        stock_value = StockValue(
            symbol=tokens[0],
            date=tokens[1],
            price=float(tokens[2]),
        )
        stock_values.append(stock_value)
    return stock_values


def main():
    sc = spark.sparkContext
    numbers = range(1, 1_000_001)
    numbers_rdd = sc.parallelize(numbers)

    even_numbers_rdd = numbers_rdd.filter(lambda n: n % 2 == 0)
    print(even_numbers_rdd.take(5))
    print(even_numbers_rdd.count())

    stocks = read_stocks("resources/data/stocks.csv")
    stocks_rdd = sc.parallelize(stocks)

    print(stocks_rdd.take(5))

    stocks_rdd_2 = (
        sc.textFile("resources/data/stocks.csv")
        .map(lambda line: line.split(","))
        .filter(lambda tokens: tokens[0].upper() == tokens[0])
        .map(lambda tokens: StockValue(tokens[0], tokens[1], float(tokens[2])))
    )

    print(stocks_rdd_2.take(5))

    stocks_df = (
        spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("resources/data/stocks.csv")
    )

    stocks_df.printSchema()
    stocks_df.show(5)
    stocks_rdd_3 = stocks_df.rdd
    print(stocks_rdd_3.take(5))

    numbers_df = numbers_rdd.map(lambda number: (number,)).toDF(["numbers"])
    numbers_df.show()

    msft_rdd = stocks_rdd_2.filter(lambda stock: stock.symbol == "MSFT")
    msft_count = msft_rdd.count()

    print("MSFT rows:", msft_count)

    company_names_rdd = (
        stocks_rdd_2
        .map(lambda stock: stock.symbol)
        .distinct()
    )

    company_names_rdd.collect()

    min_msft = msft_rdd.min(key=lambda stock: stock.price)
    print("Min MSFT price:", min_msft)

    numbers_sum = numbers_rdd.reduce(lambda a, b: a + b)
    print(f"Number sum: {numbers_sum}")

    # Exoensive op- large mem footprint
    grouped_stocks_rdd = stocks_rdd_2.groupBy(lambda stock: stock.symbol)

    for symbol, stocks in grouped_stocks_rdd.take(5):
        print(symbol, list(stocks)[:3])

    stock_counts_rdd = (
        stocks_rdd_2
        .map(lambda stock: (stock.symbol, 1))
        .reduceByKey(lambda a, b: a + b)
    )

    print(stock_counts_rdd.take(5))

    repartitioned_stocks_rdd = stocks_rdd_2.repartition(30)

    print(repartitioned_stocks_rdd.take(5))
    print(repartitioned_stocks_rdd.getNumPartitions())

    repartitioned_stocks_df = repartitioned_stocks_rdd.toDF()
    repartitioned_stocks_df.write.mode("overwrite").parquet("resources/data/repartitioned_stocks30")


if __name__ == "__main__":
    main()
