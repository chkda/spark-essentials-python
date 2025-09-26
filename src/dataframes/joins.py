import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main():
    spark = (SparkSession.builder
             .appName("Joins in pyspark")
             .config("spark.master", "local")
             .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
             .getOrCreate())

    guitars_df = (spark.read.format("json")
                  .option("inferSchema", "true")
                  .load("resources/data/guitars.json"))

    guitarists_df = (spark.read.format("json")
                     .option("inferSchema", "true")
                     .load("resources/data/guitarPlayers.json"))

    bands_df = (spark.read.format("json")
                .option("inferSchema", "true")
                .load("resources/data/bands.json"))

    join_condition = guitarists_df["band"] == bands_df["id"]
    guitarists_bands_df = guitarists_df.join(bands_df, join_condition, "inner")
    print("--------------------Inner join--------------------------")
    guitarists_bands_df.show()

    print("------------------Left Outer-----------------------------")
    guitarists_df.join(bands_df, join_condition, "left_outer").show()

    print("------------------Right Outer-----------------------------")
    guitarists_df.join(bands_df, join_condition, "right_outer").show()

    print("------------------Outer-----------------------------")
    guitarists_df.join(bands_df, join_condition, "outer").show()

    print("------------------Left Semi-----------------------------")
    guitarists_df.join(bands_df, join_condition, "left_semi").show()

    print("------------------Left Anti-----------------------------")
    guitarists_df.join(bands_df, join_condition, "left_anti").show()

    guitarists_df.join(bands_df.withColumnRenamed("id", "band"), "band").show()

    guitarists_bands_df.drop(bands_df.id).show()

    bands_mod_df = bands_df.withColumnRenamed("id", "bandId")
    guitarists_df.join(bands_mod_df, guitarists_df["band"] == bands_mod_df["bandId"]).show()

    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/rtjvm"
    user = "docker"
    password = "docker"

    def read_table(table_name: str):
        return (spark.read
                .format("jdbc")
                .option("driver", driver)
                .option("url", url)
                .option("user", user)
                .option("password", password)
                .option("dbtable", f"public.{table_name}")
                .load())

    employees_df = read_table("employees")
    salaries_df = read_table("salaries")
    dept_managers_df = read_table("dept_manager")
    title_df = read_table("titles")

    max_salaries_per_employee_df = (salaries_df
                                    .groupBy("emp_no")
                                    .agg(F.max("salary")
                                         .alias("maxSalary")))

    employees_salaries_df = employees_df.join(
        max_salaries_per_employee_df,
        "emp_no"
    ).sort(F.desc("maxSalary"))

    # employees_salaries_df.show()

    join_condition = employees_df["emp_no"] == dept_managers_df["emp_no"]
    employees_never_manager_df = employees_df.join(dept_managers_df, join_condition, "left_anti")
    # employees_never_manager_df.show()

    most_recent_job_titles_df = (title_df
                                 .groupBy("emp_no", "title")
                                 .agg(F.max("to_date")))

    best_paid_employees_df = (employees_salaries_df
                              .orderBy(F.col("maxSalary").desc())
                              .limit(10))

    best_paid_jobs_df = best_paid_employees_df.join(most_recent_job_titles_df, "emp_no")
    best_paid_jobs_df.show()


if __name__ == "__main__":
    main()
