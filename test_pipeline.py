"""
Complex lineage test: multiple sources, branches, joins, aggregations,
window functions, and a class-based pipeline — all tracked from the boundary.

Requirements:
  pip install pyspark psycopg2-binary
  PostgreSQL running locally (db: nithinrajendran, user/pass: postgres)
  Internet access for first run (downloads postgres JDBC driver from Maven)
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import tempfile
import psycopg2
import spark_lineage as spl

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, sum as _sum, count, avg, rank,
    datediff, current_date, to_date, concat_ws, round as _round, year,
)
from pyspark.sql.window import Window
from pyspark.sql import types as T


# ── Spark session ─────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("spark_lineage_test")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .config("spark.sql.shuffle.partitions", "4")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

JDBC_URL = "jdbc:postgresql://localhost:5432/nithinrajendran"
JDBC_PROPS = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}


# ── 1. Seed postgres ───────────────────────────────────────────────────────────

def seed_postgres():
    conn = psycopg2.connect(
        dbname="nithinrajendran", user="postgres", password="postgres", host="localhost"
    )
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS employees")
    cur.execute("""
        CREATE TABLE employees (
            id        INT PRIMARY KEY,
            name      TEXT,
            dept_id   INT,
            salary    NUMERIC,
            status    TEXT,
            hire_date DATE
        )
    """)
    cur.executemany(
        "INSERT INTO employees VALUES (%s,%s,%s,%s,%s,%s)",
        [
            (1,  "Alice",   10, 95000, "active",   "2018-03-15"),
            (2,  "Bob",     10, 82000, "active",   "2020-07-01"),
            (3,  "Carol",   20, 110000,"active",   "2016-11-20"),
            (4,  "Dave",    20, 78000, "inactive", "2021-02-14"),
            (5,  "Eve",     30, 125000,"active",   "2015-05-05"),
            (6,  "Frank",   30, 91000, "active",   "2019-09-30"),
            (7,  "Grace",   10, 88000, "active",   "2017-12-01"),
            (8,  "Hank",    20, 102000,"active",   "2020-04-18"),
        ],
    )
    conn.commit()
    cur.close()
    conn.close()

seed_postgres()


# ── 2. Create file-based sources ───────────────────────────────────────────────

tmpdir = tempfile.mkdtemp()

# Sales — written as CSV
sales_rows = [
    (1, "ProductA", 12000.0, "2024-01-15"),
    (1, "ProductB",  8500.0, "2024-03-22"),
    (2, "ProductA",  9300.0, "2024-02-10"),
    (3, "ProductC", 21000.0, "2024-01-05"),
    (3, "ProductA", 14500.0, "2024-04-30"),
    (5, "ProductB", 31000.0, "2024-02-28"),
    (6, "ProductC", 17800.0, "2024-03-15"),
    (6, "ProductA", 11200.0, "2024-05-01"),
    (7, "ProductB",  7600.0, "2024-01-20"),
    (8, "ProductC", 19400.0, "2024-04-10"),
    (4, "ProductA",  5000.0, "2023-11-01"),  # inactive emp, old sale
]
sales_schema = T.StructType([
    T.StructField("emp_id",     T.IntegerType()),
    T.StructField("product",    T.StringType()),
    T.StructField("amount",     T.DoubleType()),
    T.StructField("sale_date",  T.StringType()),
])
csv_path = os.path.join(tmpdir, "sales.csv")
(
    spark.createDataFrame(sales_rows, sales_schema)
    .write.mode("overwrite").option("header", True).csv(csv_path)
)

# Targets — written as Parquet
targets_rows = [
    (10, 2024, 45000.0, "Engineering"),
    (20, 2024, 60000.0, "Research"),
    (30, 2024, 75000.0, "Sales"),
]
targets_schema = T.StructType([
    T.StructField("dept_id",       T.IntegerType()),
    T.StructField("year",          T.IntegerType()),
    T.StructField("target_amount", T.DoubleType()),
    T.StructField("dept_name",     T.StringType()),
])
parquet_path = os.path.join(tmpdir, "targets")
spark.createDataFrame(targets_rows, targets_schema).write.mode("overwrite").parquet(parquet_path)


# ── 3. Pipeline class (lineage flows in via tracked DFs at boundary) ───────────

class EmployeeAnalyticsPipeline:
    """
    A realistic pipeline modelling how teams in Kedro/similar frameworks
    compose nodes. Lineage is tracked outside this class — propagation
    handles everything inside automatically.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _enrich_employees(self, employees, departments):
        """Filter, join with departments, add seniority label."""
        active = employees.filter(col("status") == "active")

        enriched = (
            active
            .join(departments, "dept_id")
            .withColumn(
                "seniority",
                when(datediff(current_date(), to_date(col("hire_date"))) > 365 * 5, "senior")
                .otherwise("junior"),
            )
            .withColumnRenamed("name", "emp_name")
            .withColumn("full_label", concat_ws(" · ", col("emp_name"), col("dept_name"), col("seniority")))
        )
        return enriched

    def _aggregate_sales(self, sales):
        """Keep only 2024 sales and roll up per employee."""
        return (
            sales
            .withColumn("sale_date", to_date(col("sale_date")))
            .filter(year(col("sale_date")) == lit(2024))
            .groupBy("emp_id")
            .agg(
                _sum("amount").alias("total_sales"),
                count("*").alias("num_sales"),
                avg("amount").alias("avg_sale"),
            )
        )

    def _combine_and_rank(self, enriched, sales_agg, targets):
        """Join both branches, apply window rank, derive bonus and target achievement."""
        w = Window.partitionBy("dept_id").orderBy(col("total_sales").desc())

        combined = (
            enriched
            .join(sales_agg, enriched["id"] == sales_agg["emp_id"], "left")
            .drop(sales_agg["emp_id"])
            .withColumn("total_sales", when(col("total_sales").isNull(), lit(0.0)).otherwise(col("total_sales")))
        )

        ranked = (
            combined
            .withColumn("dept_rank", rank().over(w))
            .withColumn(
                "bonus",
                when(col("dept_rank") == 1, _round(col("salary") * 0.20, 2))
                .when(col("dept_rank") == 2, _round(col("salary") * 0.10, 2))
                .otherwise(lit(0.0)),
            )
            .withColumn("performance_score", _round(col("total_sales") / col("salary"), 4))
        )

        targets_2024 = (
            targets
            .filter(col("year") == lit(2024))
            .select("dept_id", "target_amount")   # avoid dept_name collision
        )
        with_targets = (
            ranked
            .join(targets_2024, "dept_id", "left")
            .withColumn(
                "target_pct",
                _round(col("total_sales") / col("target_amount") * 100, 1),
            )
            .withColumn(
                "beat_target",
                when(col("target_pct") >= lit(100.0), lit(True)).otherwise(lit(False)),
            )
        )

        return with_targets

    def run(self, employees, departments, sales, targets):
        enriched   = self._enrich_employees(employees, departments)
        sales_agg  = self._aggregate_sales(sales)
        result     = self._combine_and_rank(enriched, sales_agg, targets)
        return result


# ── 4. Load sources & track at the boundary ───────────────────────────────────

employees = spl.track_df(
    spark.read.jdbc(JDBC_URL, "employees", properties=JDBC_PROPS),
    name="employees",
)

departments = spl.track_df(
    spark.createDataFrame(
        [(10, "Engineering", "NYC", 500000.0),
         (20, "Research",    "SF",  750000.0),
         (30, "Sales",       "LA",  900000.0)],
        ["dept_id", "dept_name", "location", "budget"],
    ),
    name="departments",
)

sales = spl.track_df(
    spark.read.option("header", True).schema(sales_schema).csv(csv_path),
    name="sales",
)

targets = spl.track_df(
    spark.read.parquet(parquet_path),
    name="targets",
)


# ── 5. Run pipeline and print lineage ─────────────────────────────────────────

pipeline = EmployeeAnalyticsPipeline(spark)
result   = pipeline.run(employees, departments, sales, targets)

print("\n=== RESULT SAMPLE ===")
result.select(
    "emp_name", "dept_name", "seniority",
    "total_sales", "dept_rank", "bonus",
    "target_pct", "beat_target",
).show(truncate=False)

spl.print_lineage(result)
spl.save_report(result, path="./employee_analytics_lineage", name="Employee Analytics Pipeline")
