"""
Comprehensive spark_lineage test — zero external dependencies.

Sources (all in-memory):
  orders      45 rows · 3 years · 6 products · 8 customers · 3 channels
  customers    8 rows · demographics + credit score
  products     6 rows · category / cost / price
  campaigns    4 rows · promo discounts by channel

Pipeline (class-based, 4 independent branches from orders):
  Branch A  customer_ltv        filter → join(customers) → agg → tier + days_as_customer
  Branch B  product_performance filter → join(products)  → margin → agg → rank (window)
  Branch C  monthly_trends      time dims → groupBy → lag window → MoM growth
  Branch D  channel_efficiency  join(campaigns) → ROI → agg → efficiency grade

Each branch is a leaf → all 4 appear as targets in the report.
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import spark_lineage as spl
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import types as T

# ── Spark ─────────────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("spark_lineage_comprehensive")
    .config("spark.sql.shuffle.partitions", "4")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ── Schemas ───────────────────────────────────────────────────────────────────

orders_schema = T.StructType([
    T.StructField("order_id",      T.IntegerType()),
    T.StructField("customer_id",   T.IntegerType()),
    T.StructField("product_id",    T.StringType()),
    T.StructField("quantity",      T.IntegerType()),
    T.StructField("unit_price",    T.DoubleType()),
    T.StructField("discount_rate", T.DoubleType()),
    T.StructField("order_date",    T.StringType()),
    T.StructField("channel",       T.StringType()),
    T.StructField("status",        T.StringType()),
])

customer_schema = T.StructType([
    T.StructField("customer_id",  T.IntegerType()),
    T.StructField("name",         T.StringType()),
    T.StructField("country",      T.StringType()),
    T.StructField("age",          T.IntegerType()),
    T.StructField("segment",      T.StringType()),
    T.StructField("credit_score", T.IntegerType()),
    T.StructField("join_date",    T.StringType()),
])

product_schema = T.StructType([
    T.StructField("product_id",  T.StringType()),
    T.StructField("product_name",T.StringType()),
    T.StructField("category",    T.StringType()),
    T.StructField("unit_cost",   T.DoubleType()),
    T.StructField("list_price",  T.DoubleType()),
    T.StructField("is_active",   T.BooleanType()),
])

campaign_schema = T.StructType([
    T.StructField("campaign_id",    T.StringType()),
    T.StructField("channel",        T.StringType()),
    T.StructField("bonus_discount", T.DoubleType()),
    T.StructField("budget_usd",     T.DoubleType()),
    T.StructField("start_date",     T.StringType()),
    T.StructField("end_date",       T.StringType()),
])

# ── Data ──────────────────────────────────────────────────────────────────────

orders_data = [
    # 2022
    (1001, 1, "P001",  2, 199.99, 0.00, "2022-02-14", "web",    "completed"),
    (1002, 2, "P002",  1, 499.00, 0.05, "2022-04-01", "mobile", "completed"),
    (1003, 3, "P003",  3,  89.99, 0.10, "2022-06-18", "web",    "completed"),
    (1004, 4, "P004",  1, 850.00, 0.00, "2022-08-22", "store",  "completed"),
    (1005, 5, "P001",  4, 199.99, 0.15, "2022-10-05", "mobile", "completed"),
    (1006, 6, "P005",  2, 320.00, 0.00, "2022-11-11", "web",    "cancelled"),
    (1007, 7, "P002",  1, 499.00, 0.00, "2022-12-24", "store",  "completed"),
    # 2023
    (1008, 1, "P003",  5,  89.99, 0.05, "2023-01-10", "web",    "completed"),
    (1009, 2, "P001",  2, 199.99, 0.10, "2023-02-28", "mobile", "completed"),
    (1010, 3, "P004",  1, 850.00, 0.00, "2023-03-15", "web",    "completed"),
    (1011, 4, "P006",  3, 149.00, 0.05, "2023-04-20", "store",  "completed"),
    (1012, 5, "P002",  1, 499.00, 0.20, "2023-05-01", "web",    "completed"),
    (1013, 6, "P003",  2,  89.99, 0.00, "2023-06-14", "mobile", "completed"),
    (1014, 7, "P005",  1, 320.00, 0.10, "2023-07-04", "store",  "completed"),
    (1015, 8, "P001",  3, 199.99, 0.05, "2023-08-19", "web",    "completed"),
    (1016, 1, "P004",  2, 850.00, 0.00, "2023-09-30", "mobile", "completed"),
    (1017, 2, "P006",  4, 149.00, 0.10, "2023-10-12", "web",    "completed"),
    (1018, 3, "P002",  1, 499.00, 0.05, "2023-11-25", "store",  "refunded"),
    (1019, 8, "P003",  6,  89.99, 0.15, "2023-12-01", "mobile", "completed"),
    # 2024
    (1020, 1, "P001",  3, 219.99, 0.00, "2024-01-08", "web",    "completed"),
    (1021, 2, "P002",  2, 519.00, 0.05, "2024-01-20", "mobile", "completed"),
    (1022, 3, "P003",  4,  99.99, 0.10, "2024-02-03", "web",    "completed"),
    (1023, 4, "P004",  1, 899.00, 0.00, "2024-02-17", "store",  "completed"),
    (1024, 5, "P005",  3, 349.00, 0.05, "2024-03-01", "web",    "completed"),
    (1025, 6, "P006",  5, 159.00, 0.10, "2024-03-14", "mobile", "completed"),
    (1026, 7, "P001",  2, 219.99, 0.15, "2024-04-02", "store",  "completed"),
    (1027, 8, "P002",  1, 519.00, 0.00, "2024-04-18", "web",    "completed"),
    (1028, 1, "P003",  3,  99.99, 0.05, "2024-05-05", "mobile", "completed"),
    (1029, 2, "P004",  2, 899.00, 0.10, "2024-05-22", "web",    "completed"),
    (1030, 3, "P005",  1, 349.00, 0.00, "2024-06-07", "store",  "completed"),
    (1031, 4, "P006",  4, 159.00, 0.05, "2024-06-25", "mobile", "completed"),
    (1032, 5, "P001",  5, 219.99, 0.20, "2024-07-11", "web",    "completed"),
    (1033, 6, "P002",  1, 519.00, 0.00, "2024-07-28", "store",  "completed"),
    (1034, 7, "P003",  2,  99.99, 0.10, "2024-08-14", "mobile", "completed"),
    (1035, 8, "P004",  1, 899.00, 0.05, "2024-08-30", "web",    "completed"),
    (1036, 1, "P005",  2, 349.00, 0.15, "2024-09-12", "store",  "completed"),
    (1037, 2, "P006",  3, 159.00, 0.00, "2024-10-01", "web",    "completed"),
    (1038, 3, "P001",  4, 219.99, 0.05, "2024-10-20", "mobile", "completed"),
    (1039, 4, "P002",  1, 519.00, 0.10, "2024-11-06", "web",    "cancelled"),
    (1040, 5, "P003",  3,  99.99, 0.00, "2024-11-22", "store",  "completed"),
    (1041, 6, "P004",  2, 899.00, 0.20, "2024-12-05", "mobile", "completed"),
    (1042, 7, "P005",  1, 349.00, 0.00, "2024-12-15", "web",    "completed"),
    (1043, 8, "P006",  5, 159.00, 0.05, "2024-12-28", "store",  "completed"),
    (1044, 1, "P002",  1, 519.00, 0.10, "2024-12-30", "mobile", "completed"),
    (1045, 2, "P001",  2, 219.99, 0.15, "2024-12-31", "web",    "completed"),
]

customers_data = [
    (1, "Alice Chen",     "US", 34, "premium",  810, "2020-06-01"),
    (2, "Bob Martin",     "UK", 45, "standard", 720, "2021-03-15"),
    (3, "Carol Wu",       "US", 29, "premium",  790, "2020-11-20"),
    (4, "David Lopez",    "MX", 52, "standard", 650, "2022-01-08"),
    (5, "Eve Nakamura",   "JP", 38, "vip",      880, "2019-09-01"),
    (6, "Frank Schmidt",  "DE", 41, "standard", 695, "2021-07-14"),
    (7, "Grace Kim",      "KR", 27, "premium",  755, "2022-05-30"),
    (8, "Hank Williams",  "US", 60, "vip",      830, "2018-12-01"),
]

products_data = [
    ("P001", "AirBuds Pro",     "Electronics", 65.00,  219.99, True),
    ("P002", "SmartWatch X",    "Electronics", 180.00, 519.00, True),
    ("P003", "Yoga Mat Elite",  "Fitness",      18.00,  99.99, True),
    ("P004", "Espresso Master", "Kitchen",     320.00, 899.00, True),
    ("P005", "Trail Runner 5",  "Footwear",    110.00, 349.00, True),
    ("P006", "Memory Foam+",    "Home",         42.00, 159.00, True),
]

campaigns_data = [
    ("C01", "web",    0.05, 50000.0,  "2023-01-01", "2023-12-31"),
    ("C02", "mobile", 0.08, 40000.0,  "2023-01-01", "2024-06-30"),
    ("C03", "store",  0.03, 25000.0,  "2024-01-01", "2024-12-31"),
    ("C04", "web",    0.10, 80000.0,  "2024-01-01", "2024-12-31"),
]


# ── Pipeline ──────────────────────────────────────────────────────────────────

class SalesAnalyticsPipeline:
    """
    Four independent analysis branches, all rooted in `orders`.
    No branch reuses another's output — all four are leaf targets.
    """

    def customer_lifetime_value(self, orders, customers):
        """Branch A: Customer LTV segmentation."""
        completed = (
            orders
            .filter(F.col("status") == "completed")
            .withColumn("order_dt", F.to_date(F.col("order_date")))
        )

        # Net revenue per order line
        with_revenue = completed.withColumn(
            "line_revenue",
            F.round(
                F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_rate")),
                2,
            ),
        )

        # Join demographics
        enriched = with_revenue.join(customers, "customer_id")

        # Aggregate lifetime metrics
        ltv_raw = (
            enriched
            .groupBy("customer_id", "name", "country", "segment", "credit_score")
            .agg(
                F.round(F.sum("line_revenue"), 2).alias("lifetime_value"),
                F.count("*").alias("order_count"),
                F.round(F.avg("line_revenue"), 2).alias("avg_order_value"),
                F.min("order_dt").alias("first_order_date"),
                F.max("order_dt").alias("last_order_date"),
                F.round(F.avg("discount_rate") * 100, 1).alias("avg_discount_pct"),
            )
        )

        # Intermediate: spend_per_order — lifetime_value divided by order_count.
        # Used subsequently to drive tier logic alongside lifetime_value.
        with_spend_rate = ltv_raw.withColumn(
            "spend_per_order",
            F.round(F.col("lifetime_value") / F.col("order_count"), 2),
        )

        # Tier + tenure (tier now also considers spend_per_order as a signal)
        result = (
            with_spend_rate
            .withColumn(
                "tier",
                F.when(
                    (F.col("lifetime_value") >= 3000) | (F.col("spend_per_order") >= 600),
                    "Gold",
                )
                .when(
                    (F.col("lifetime_value") >= 1200) | (F.col("spend_per_order") >= 300),
                    "Silver",
                )
                .otherwise("Bronze"),
            )
            .withColumn(
                "tenure_days",
                F.datediff(F.col("last_order_date"), F.col("first_order_date")),
            )
            .withColumn(
                "orders_per_month",
                F.round(
                    F.col("order_count") / F.greatest(F.col("tenure_days") / 30, F.lit(1)),
                    2,
                ),
            )
            .orderBy(F.col("lifetime_value").desc())
        )
        return result

    def product_performance(self, orders, products):
        """Branch B: Product margin and category ranking."""
        # Active-product completed orders only
        active_orders = (
            orders
            .filter(F.col("status") == "completed")
            .join(products.filter(F.col("is_active")), "product_id")
        )

        # Per-order margin
        with_margin = active_orders.withColumn(
            "gross_profit",
            F.round(
                F.col("quantity") * (F.col("unit_price") - F.col("unit_cost")),
                2,
            ),
        )

        # Roll up by product
        by_product = (
            with_margin
            .groupBy("product_id", "product_name", "category", "list_price", "unit_cost")
            .agg(
                F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("total_revenue"),
                F.round(F.sum("gross_profit"), 2).alias("total_gross_profit"),
                F.sum("quantity").alias("units_sold"),
                F.count("*").alias("num_orders"),
                F.countDistinct("customer_id").alias("unique_buyers"),
            )
        )

        # Derived rates
        with_rates = (
            by_product
            .withColumn(
                "margin_pct",
                F.round(F.col("total_gross_profit") / F.col("total_revenue") * 100, 1),
            )
            .withColumn(
                "avg_units_per_order",
                F.round(F.col("units_sold") / F.col("num_orders"), 2),
            )
        )

        # Category rank by revenue
        w_cat = Window.partitionBy("category").orderBy(F.col("total_revenue").desc())

        result = (
            with_rates
            .withColumn("category_rank", F.rank().over(w_cat))
            .withColumn("is_category_leader", F.col("category_rank") == 1)
            .orderBy("category", "category_rank")
        )
        return result

    def monthly_channel_trends(self, orders):
        """Branch C: Monthly revenue trends with MoM growth per channel."""
        # Time dimensions
        with_time = (
            orders
            .filter(F.col("status") == "completed")
            .withColumn("order_dt", F.to_date(F.col("order_date")))
            .withColumn("year_month", F.date_format(F.col("order_dt"), "yyyy-MM"))
            .withColumn("quarter",
                F.concat(
                    F.year(F.col("order_dt")).cast("string"),
                    F.lit("-Q"),
                    F.ceil(F.month(F.col("order_dt")) / 3).cast("string"),
                )
            )
        )

        # Intermediate: raw line revenue before discount, computed per-row before aggregation
        with_raw = with_time.withColumn(
            "raw_revenue",
            F.round(F.col("quantity") * F.col("unit_price"), 2),
        )

        # Revenue per month × channel (net_revenue uses raw_revenue as its base)
        monthly = (
            with_raw
            .withColumn(
                "net_revenue",
                F.round(F.col("raw_revenue") * (1 - F.col("discount_rate")), 2),
            )
            .groupBy("year_month", "quarter", "channel")
            .agg(
                F.round(F.sum("net_revenue"), 2).alias("revenue"),
                F.round(F.sum("raw_revenue"), 2).alias("gross_revenue"),
                F.count("*").alias("order_count"),
                F.countDistinct("customer_id").alias("active_customers"),
                F.round(F.avg("discount_rate") * 100, 2).alias("avg_discount_pct"),
            )
            .orderBy("year_month", "channel")
        )

        # MoM growth via lag
        w_ch = Window.partitionBy("channel").orderBy("year_month")

        result = (
            monthly
            .withColumn("prev_revenue", F.lag("revenue", 1).over(w_ch))
            .withColumn(
                "mom_growth_pct",
                F.round(
                    (F.col("revenue") - F.col("prev_revenue")) / F.col("prev_revenue") * 100,
                    1,
                ),
            )
            .withColumn(
                "growth_flag",
                F.when(F.col("mom_growth_pct") > 10, "strong_growth")
                .when(F.col("mom_growth_pct") > 0,  "growth")
                .when(F.col("mom_growth_pct").isNull(), "first_period")
                .otherwise("decline"),
            )
        )
        return result

    def channel_campaign_efficiency(self, orders, campaigns):
        """Branch D: Campaign ROI and channel efficiency scoring."""
        # Mark each order with the campaign active on that date for that channel
        with_dt = orders.filter(F.col("status") == "completed").withColumn(
            "order_dt", F.to_date(F.col("order_date"))
        )

        # Cross join then filter to matching channel + date window
        with_campaign = (
            with_dt
            .join(campaigns, on="channel")
            .filter(
                (F.col("order_dt") >= F.to_date(F.col("start_date")))
                & (F.col("order_dt") <= F.to_date(F.col("end_date")))
            )
        )

        # Intermediate: per-order budget weight (fraction of budget this order "consumes")
        # Used in downstream aggregation to compute cost_per_impression proxy
        with_net = with_campaign.withColumn(
            "combined_discount",
            F.least(F.col("discount_rate") + F.col("bonus_discount"), F.lit(0.40)),
        ).withColumn(
            "campaign_net_revenue",
            F.round(
                F.col("quantity") * F.col("unit_price") * (1 - F.col("combined_discount")),
                2,
            ),
        )

        # Aggregate by channel + campaign
        by_channel = (
            with_net
            .groupBy("channel", "campaign_id", "budget_usd")
            .agg(
                F.round(F.sum("campaign_net_revenue"), 2).alias("attributed_revenue"),
                F.count("*").alias("orders_in_campaign"),
                F.countDistinct("customer_id").alias("reached_customers"),
                F.round(F.avg("combined_discount") * 100, 1).alias("effective_discount_pct"),
            )
        )

        # Intermediate: cost_per_order computed before efficiency grade uses it
        with_cost = by_channel.withColumn(
            "cost_per_order",
            F.round(F.col("budget_usd") / F.col("orders_in_campaign"), 2),
        )

        # Intermediate: cost_per_customer derived from cost_per_order × orders_in_campaign / reached_customers
        with_cost_per_customer = with_cost.withColumn(
            "cost_per_customer",
            F.round(F.col("budget_usd") / F.col("reached_customers"), 2),
        )

        # ROI and efficiency grade (efficiency_grade uses roi_pct which is a new intermediate)
        result = (
            with_cost_per_customer
            .withColumn(
                "roi_pct",
                F.round(
                    (F.col("attributed_revenue") - F.col("budget_usd"))
                    / F.col("budget_usd") * 100,
                    1,
                ),
            )
            .withColumn(
                "efficiency_grade",
                F.when(F.col("roi_pct") >= 200, "A")
                .when(F.col("roi_pct") >= 100, "B")
                .when(F.col("roi_pct") >= 0,   "C")
                .otherwise("D"),
            )
            .orderBy(F.col("roi_pct").desc())
        )
        return result

    def run(self, orders, customers, products, campaigns):
        ltv      = self.customer_lifetime_value(orders, customers)
        perf     = self.product_performance(orders, products)
        trends   = self.monthly_channel_trends(orders)
        campaign = self.channel_campaign_efficiency(orders, campaigns)
        return ltv, perf, trends, campaign


# ── Sources — track at the boundary ──────────────────────────────────────────

def load_sources(spark):
    """
    Load all in-memory source DataFrames and register them with spark_lineage.
    Calling track_df inside a named function gives a meaningful FQN in the report,
    e.g. ``test_comprehensive.load_sources:NNN`` instead of bare ``test_comprehensive:NNN``.
    """
    orders = spl.track_df(
        spark.createDataFrame(orders_data, orders_schema),
        name="orders",
    )

    customers = spl.track_df(
        spark.createDataFrame(customers_data, customer_schema),
        name="customers",
    )

    products = spl.track_df(
        spark.createDataFrame(products_data, product_schema),
        name="products",
    )

    campaigns = spl.track_df(
        spark.createDataFrame(campaigns_data, campaign_schema),
        name="campaigns",
    )

    return orders, customers, products, campaigns


# ── Run pipeline + write outputs ──────────────────────────────────────────────

import tempfile, os as _os
_out = _os.path.join(tempfile.gettempdir(), "spark_lineage_test")

orders, customers, products, campaigns = load_sources(spark)
pipeline = SalesAnalyticsPipeline()
ltv, perf, trends, campaign_eff = pipeline.run(orders, customers, products, campaigns)

# Write outputs — these become the lineage targets instead of Python variables
ltv.write.mode("overwrite").parquet(_os.path.join(_out, "customer_lifetime_value"))
perf.write.mode("overwrite").parquet(_os.path.join(_out, "product_performance"))
trends.write.mode("overwrite").parquet(_os.path.join(_out, "monthly_channel_trends"))
campaign_eff.write.mode("overwrite").parquet(_os.path.join(_out, "channel_campaign_efficiency"))

# ── SQL branch (E): register temp views then query via spark.sql() ─────────────
# This tests Spark SQL compatibility: views of tracked DataFrames are resolved
# back to their lineage parents transparently.

orders.createOrReplaceTempView("orders_v")
customers.createOrReplaceTempView("customers_v")
products.createOrReplaceTempView("products_v")

sql_channel_segment = spark.sql("""
    SELECT
        o.channel,
        c.segment,
        COUNT(*)                                                        AS order_count,
        ROUND(SUM(o.quantity * o.unit_price * (1 - o.discount_rate)), 2) AS net_revenue,
        ROUND(AVG(o.discount_rate) * 100, 1)                            AS avg_discount_pct
    FROM orders_v   o
    JOIN customers_v c ON o.customer_id = c.customer_id
    WHERE o.status = 'completed'
    GROUP BY o.channel, c.segment
    ORDER BY net_revenue DESC
""")
sql_channel_segment.write.mode("overwrite").parquet(
    _os.path.join(_out, "sql_channel_segment")
)

sql_top_products = spark.sql("""
    SELECT
        p.product_name,
        p.category,
        SUM(o.quantity)                                           AS units_sold,
        ROUND(SUM(o.quantity * o.unit_price), 2)                  AS gross_revenue,
        ROUND(SUM(o.quantity * (o.unit_price - p.unit_cost)), 2)  AS gross_profit
    FROM orders_v   o
    JOIN products_v p ON o.product_id = p.product_id
    WHERE o.status = 'completed'
    GROUP BY p.product_name, p.category
    ORDER BY gross_profit DESC
""")
sql_top_products.write.mode("overwrite").parquet(
    _os.path.join(_out, "sql_top_products")
)

# ── Post-write continuation (F): lineage continues after a write ───────────────
# Verifies that writing a DataFrame to storage and then continuing to transform
# it produces two independent write targets — both appear in the lineage graph.

enriched_orders = (
    orders
    .filter(F.col("status") == "completed")
    .withColumn("line_revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
)
enriched_orders.write.mode("overwrite").parquet(
    _os.path.join(_out, "enriched_orders")
)

# Continue transforming AFTER the write — this must also appear as a target.
summary_orders = (
    enriched_orders
    .groupBy("channel")
    .agg(
        F.round(F.sum("line_revenue"), 2).alias("channel_revenue"),
        F.count("*").alias("order_count"),
    )
)
summary_orders.write.mode("overwrite").parquet(
    _os.path.join(_out, "channel_summary")
)

# ── Generate report ────────────────────────────────────────────────────────────

spl.save_report(orders, path="./sales_lineage", name="Sales Analytics Pipeline")

# ── Show samples ──────────────────────────────────────────────────────────────

print("\n=== Customer LTV ===")
ltv.show(8, truncate=False)

print("\n=== Product Performance ===")
perf.show(6, truncate=False)

print("\n=== Monthly Trends (sample) ===")
trends.show(10, truncate=False)

print("\n=== Campaign Efficiency ===")
campaign_eff.show(4, truncate=False)

print("\nDone.")
