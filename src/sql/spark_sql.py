from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Spark SQL")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "resources/warehouse")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
    .getOrCreate()
)

driver = "org.postgresql.Driver"
url = "jdbc:postgresql://localhost:5432/rtjvm"
user = "docker"
password = "docker"


def read_table(table_name: str):
    return (
        spark.read
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", f"public.{table_name}")
        .load()
    )


def transfer_tables(table_names: list[str], should_wrtte_to_warehouse: bool = False):
    for table_name in table_names:
        table_df = read_table(table_name)
        table_df.createOrReplaceTempView(table_name)
        if should_wrtte_to_warehouse:
            table_df.write.mode("overwrite").saveAsTable(table_name)


def main():
    cars_df = (
        spark.read
        .option("inferSchema", "true")
        .json("resources/data/cars.json")
    )

    cars_df.printSchema()
    cars_df.show()

    cars_df.createOrReplaceTempView("cars")
    american_cars_with_sql = spark.sql("""
    select Name 
    from cars
    where Origin = 'USA'
    """)

    american_cars_with_sql.show()

    spark.sql("create database if not exists rtjvm")
    spark.sql("use rtjvm")

    database_df = spark.sql("show databases")
    database_df.show()

    # movies_df = (
    #     spark.read
    #     .option("inferSchema", "true")
    #     .json("resources/data/movies.json")
    # )

    # spark.sql("drop table if exists movies")

    # movies_df.write.mode("overwrite").saveAsTable("moviesV2")
    #
    # movies_df = spark.sql("select * from moviesV2")
    # movies_df.show()

    transfer_tables([
        "employees",
        "departments",
        "titles",
        "dept_emp",
        "salaries",
        "dept_manager",
    ])

    spark.sql("select * from employees").show()
    spark.sql("select * from departments").show()


if __name__ == "__main__":
    main()
