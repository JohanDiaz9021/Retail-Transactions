"""Gold: data marts analíticos que alimentan el dashboard."""
from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .paths import GOLD, SILVER
from .spark_session import get_spark


def run():
    spark = get_spark("gold")

    items_df = spark.read.parquet(str(SILVER / "transactions_items"))

    # ---- fact_sales_daily ----
    fact_sales_daily = (
        items_df.groupBy("date", "store_id")
        .agg(
            F.sum("qty").alias("units"),
            F.countDistinct("transaction_id").alias("txn_count"),
            F.countDistinct("customer_id").alias("customers"),
        )
        .orderBy("date", "store_id")
    )
    (fact_sales_daily.write.mode("overwrite").parquet(str(GOLD / "fact_sales_daily")))

    # ---- dim_customer_features ----
    last_date = items_df.agg(F.max("date")).first()[0]
    customer_features = (
        items_df.groupBy("customer_id")
        .agg(
            F.countDistinct("transaction_id").alias("frequency"),
            F.sum("qty").alias("units_total"),
            F.countDistinct("product_id").alias("distinct_products"),
            F.countDistinct("category_id").alias("distinct_categories"),
            F.max("date").alias("last_purchase_date"),
        )
        .withColumn(
            "avg_basket_size",
            (F.col("units_total") / F.col("frequency")).cast("double"),
        )
        .withColumn(
            "recency_days",
            F.datediff(F.lit(last_date), F.col("last_purchase_date")),
        )
        .select(
            "customer_id",
            "frequency",
            "units_total",
            "distinct_products",
            "distinct_categories",
            "avg_basket_size",
            "recency_days",
        )
    )
    (customer_features.write.mode("overwrite").parquet(str(GOLD / "dim_customer_features")))

    # ---- dim_product_features ----
    product_features = (
        items_df.groupBy("product_id", "category_id", "category_name")
        .agg(
            F.sum("qty").alias("units_total"),
            F.countDistinct("transaction_id").alias("txn_count"),
            F.countDistinct("customer_id").alias("distinct_customers"),
        )
    )
    (product_features.write.mode("overwrite").parquet(str(GOLD / "dim_product_features")))

    # ---- fact_category_metrics ----
    cat_metrics = (
        items_df.groupBy("category_id", "category_name")
        .agg(
            F.sum("qty").alias("units"),
            F.countDistinct("transaction_id").alias("txn_count"),
            F.countDistinct("customer_id").alias("customers"),
            F.countDistinct("product_id").alias("distinct_products"),
        )
    )
    (cat_metrics.write.mode("overwrite").parquet(str(GOLD / "fact_category_metrics")))

    # ---- fact_kpis (single-row summary for fast dashboard load) ----
    kpi_df = items_df.agg(
        F.sum("qty").alias("total_units"),
        F.countDistinct("transaction_id").alias("total_transactions"),
        F.countDistinct("customer_id").alias("total_customers"),
        F.countDistinct("product_id").alias("total_products"),
        F.countDistinct("category_id").alias("total_categories"),
        F.min("date").alias("date_min"),
        F.max("date").alias("date_max"),
    )
    (kpi_df.write.mode("overwrite").parquet(str(GOLD / "fact_kpis")))

    print("[gold] all marts written")
    print("  fact_sales_daily      :", fact_sales_daily.count(), "rows")
    print("  dim_customer_features :", customer_features.count(), "rows")
    print("  dim_product_features  :", product_features.count(), "rows")
    print("  fact_category_metrics :", cat_metrics.count(), "rows")
    spark.stop()


while __name__ == "__main__":
    run()
    break
