# Databricks notebook source
# MAGIC %md
# MAGIC # Illumia Campus Platform - Declarative Pipeline (DLT)
# MAGIC
# MAGIC This notebook defines a Delta Live Tables pipeline that processes data through the medallion architecture:
# MAGIC - **Bronze**: Raw data ingestion with Autoloader
# MAGIC - **Silver**: Cleaned, validated, and enriched data with cross-domain joins
# MAGIC - **Gold**: Business-level aggregations for analytics
# MAGIC
# MAGIC **AWS Equivalent**: Multiple Glue ETL jobs + Step Functions orchestration + Glue Data Quality rules

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Get configuration from pipeline settings
# CATALOG/SCHEMA define where source data volumes are located
# The pipeline's catalog/target settings determine where tables are created
CATALOG = spark.conf.get("pipeline.catalog", "illumia_demo_catalog")
SCHEMA = spark.conf.get("pipeline.schema", "workshop_data")

VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}"
CHECKPOINT_BASE = f"{VOLUME_BASE}/checkpoints"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC Autoloader ingests raw JSON files from volumes with schema inference and evolution.
# MAGIC
# MAGIC **AWS Equivalent**: S3 Events + Lambda + Glue Crawlers (x4 systems)

# COMMAND ----------

@dlt.table(
    name="bronze_cardholders",
    comment="Raw cardholder data from CS Gold identity management system",
    table_properties={"quality": "bronze"}
)
def bronze_cardholders():
    """Ingest cardholder records - student/staff identity data."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/bronze/cardholders")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{VOLUME_BASE}/cardholders/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

@dlt.table(
    name="bronze_transactions",
    comment="Raw transaction data from GET POS platform",
    table_properties={"quality": "bronze"}
)
def bronze_transactions():
    """Ingest transaction records - purchases, refunds, meal swipes."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/bronze/transactions")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{VOLUME_BASE}/transactions/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

@dlt.table(
    name="bronze_access_events",
    comment="Raw access events from CS Access door control system",
    table_properties={"quality": "bronze"}
)
def bronze_access_events():
    """Ingest access events - building entry/exit and security events."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/bronze/access_events")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{VOLUME_BASE}/access_events/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

@dlt.table(
    name="bronze_food_service",
    comment="Raw food service data from NetMenu operations",
    table_properties={"quality": "bronze"}
)
def bronze_food_service():
    """Ingest food service records - production, waste, menu items."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/bronze/food_service")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{VOLUME_BASE}/food_service/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Cleaned & Enriched
# MAGIC
# MAGIC Data quality expectations validate incoming data. Cross-domain joins enrich transactions with cardholder demographics.
# MAGIC
# MAGIC **AWS Equivalent**: Glue ETL jobs with data quality rules + multiple join operations

# COMMAND ----------

@dlt.table(
    name="silver_cardholders",
    comment="Cleaned and deduplicated cardholder dimension",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_cardholder_id", "cardholder_id IS NOT NULL")
@dlt.expect_or_drop("valid_status", "status IN ('active', 'inactive', 'suspended', 'graduated')")
@dlt.expect("has_email", "email IS NOT NULL")
@dlt.expect("has_institution", "institution_id IS NOT NULL")
def silver_cardholders():
    """Clean cardholder records with data quality validation."""
    return (
        dlt.read_stream("bronze_cardholders")
        .withColumn("patron_type_clean",
            when(col("patron_type").isin("student", "staff", "faculty", "visitor"), col("patron_type"))
            .otherwise("other"))
        .withColumn("is_active", col("status") == "active")
        .dropDuplicates(["cardholder_id"])
    )

# COMMAND ----------

@dlt.table(
    name="silver_transactions",
    comment="Enriched transactions joined with cardholder demographics",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0 AND amount < 10000")
@dlt.expect("valid_cardholder", "cardholder_id IS NOT NULL")
@dlt.expect_all_or_fail({
    "valid_merchant": "merchant_id IS NOT NULL",
    "valid_timestamp": "timestamp IS NOT NULL"
})
def silver_transactions():
    """Enrich transactions with cardholder data and time features."""
    # Get cardholder dimension for enrichment
    cardholders = dlt.read("silver_cardholders").select(
        "cardholder_id",
        "patron_type_clean",
        "housing_area",
        "department",
        "institution_id"
    ).alias("ch")

    return (
        dlt.read_stream("bronze_transactions")
        .alias("txn")
        .withColumn("transaction_date", to_date(col("timestamp")))
        .withColumn("transaction_hour", hour(col("timestamp")))
        .withColumn("day_of_week", dayofweek(col("timestamp")))
        .withColumn("day_name", date_format(col("timestamp"), "EEEE"))
        .withColumn("is_weekend", dayofweek(col("timestamp")).isin(1, 7))
        .withColumn("is_lunch_hour", (hour(col("timestamp")) >= 11) & (hour(col("timestamp")) <= 14))
        .withColumn("is_dinner_hour", (hour(col("timestamp")) >= 17) & (hour(col("timestamp")) <= 20))
        .join(cardholders, col("txn.cardholder_id") == col("ch.cardholder_id"), "left")
        .drop(col("ch.cardholder_id"))
        .drop(col("ch.institution_id"))
        .dropDuplicates(["transaction_id"])
    )

# COMMAND ----------

@dlt.table(
    name="silver_access_events",
    comment="Enriched access events with cardholder and building context",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect("valid_door", "door_id IS NOT NULL")
@dlt.expect("valid_building", "building_id IS NOT NULL")
def silver_access_events():
    """Enrich access events with cardholder data and time analysis."""
    cardholders = dlt.read("silver_cardholders").select(
        "cardholder_id",
        "patron_type_clean",
        "housing_area"
    ).alias("ch")

    return (
        dlt.read_stream("bronze_access_events")
        .alias("acc")
        .withColumn("event_date", to_date(col("timestamp")))
        .withColumn("event_hour", hour(col("timestamp")))
        .withColumn("day_of_week", dayofweek(col("timestamp")))
        .withColumn("is_after_hours", (hour(col("timestamp")) < 6) | (hour(col("timestamp")) > 22))
        .withColumn("is_weekend", dayofweek(col("timestamp")).isin(1, 7))
        .withColumn("is_denied", col("event_type") == "access_denied")
        .join(cardholders, col("acc.cardholder_id") == col("ch.cardholder_id"), "left")
        .drop(col("ch.cardholder_id"))
        .dropDuplicates(["event_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Business Analytics
# MAGIC
# MAGIC Aggregated tables optimized for dashboards and reporting. Cross-domain insights combine commerce, access, and operations data.
# MAGIC
# MAGIC **AWS Equivalent**: Additional Glue ETL jobs for aggregation + Athena views

# COMMAND ----------

@dlt.table(
    name="gold_cardholder_360",
    comment="360-degree view of each cardholder combining transactions and access",
    table_properties={"quality": "gold"}
)
def gold_cardholder_360():
    """Unified cardholder analytics - the cross-domain 'customer 360' view."""

    # Transaction summary per cardholder
    txn_summary = (
        dlt.read("silver_transactions")
        .groupBy("cardholder_id")
        .agg(
            count("transaction_id").alias("total_transactions"),
            sum("amount").alias("total_spend"),
            avg("amount").alias("avg_transaction"),
            countDistinct("merchant_category").alias("categories_used"),
            countDistinct("merchant_name").alias("unique_merchants"),
            max("transaction_date").alias("last_transaction_date")
        )
    )

    # Access summary per cardholder
    access_summary = (
        dlt.read("silver_access_events")
        .groupBy("cardholder_id")
        .agg(
            count("event_id").alias("total_access_events"),
            countDistinct("building_id").alias("unique_buildings"),
            sum(when(col("is_denied"), 1).otherwise(0)).alias("denied_count"),
            sum(when(col("is_after_hours"), 1).otherwise(0)).alias("after_hours_count"),
            avg("event_hour").alias("avg_access_hour")
        )
    )

    # Join with cardholder dimension
    cardholders = dlt.read("silver_cardholders")

    return (
        cardholders
        .join(txn_summary, "cardholder_id", "left")
        .join(access_summary, "cardholder_id", "left")
        .withColumn("engagement_score",
            (coalesce(col("total_transactions"), lit(0)) * 2) +
            (coalesce(col("total_access_events"), lit(0)) * 0.1) +
            (coalesce(col("unique_buildings"), lit(0)) * 5)
        )
        .withColumn("engagement_tier",
            when(col("engagement_score") >= 500, "high")
            .when(col("engagement_score") >= 100, "medium")
            .otherwise("low")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_location_analytics",
    comment="Revenue and traffic analytics by location and time",
    table_properties={"quality": "gold"}
)
def gold_location_analytics():
    """Location performance metrics for commerce analytics."""
    return (
        dlt.read("silver_transactions")
        .groupBy(
            "merchant_name",
            "merchant_category",
            "location_area",
            "transaction_date"
        )
        .agg(
            count("transaction_id").alias("transaction_count"),
            sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_transaction"),
            countDistinct("cardholder_id").alias("unique_customers"),
            sum(when(col("is_weekend"), col("amount")).otherwise(0)).alias("weekend_revenue"),
            sum(when(col("is_lunch_hour"), col("amount")).otherwise(0)).alias("lunch_revenue"),
            sum(when(col("is_dinner_hour"), col("amount")).otherwise(0)).alias("dinner_revenue")
        )
        .withColumn("revenue_per_customer", col("total_revenue") / col("unique_customers"))
    )

# COMMAND ----------

@dlt.table(
    name="gold_dining_operations",
    comment="Food service operational metrics - production and waste analysis",
    table_properties={"quality": "gold"}
)
def gold_dining_operations():
    """Dining hall operational efficiency and waste tracking."""
    return (
        dlt.read("bronze_food_service")
        .groupBy("location_name", "service_date", "meal_period")
        .agg(
            sum("planned_portions").alias("total_planned"),
            sum("portions_served").alias("total_served"),
            sum("portions_wasted").alias("total_wasted"),
            (sum("portions_wasted") / sum("planned_portions") * 100).alias("waste_percentage"),
            sum(col("food_cost_per_portion") * col("portions_served")).alias("total_food_cost"),
            countDistinct("menu_item_name").alias("menu_items_offered"),
            avg("calories").alias("avg_calories_per_item")
        )
        .withColumn("efficiency_rate", (col("total_served") / col("total_planned") * 100))
        .withColumn("cost_per_portion_served", col("total_food_cost") / col("total_served"))
    )

# COMMAND ----------

@dlt.table(
    name="gold_behavior_patterns",
    comment="Cross-domain behavioral insights - the analytics product differentiator",
    table_properties={"quality": "gold"}
)
def gold_behavior_patterns():
    """
    The 'analytics product' - correlations across domains.
    Example insight: 'Students in North Campus housing spend 25% more on dining than South Campus.'
    """
    return (
        dlt.read("silver_transactions")
        .filter(col("merchant_category") == "dining")
        .filter(col("housing_area").isNotNull())
        .groupBy("housing_area", "patron_type_clean")
        .agg(
            count("transaction_id").alias("transaction_count"),
            sum("amount").alias("total_spend"),
            avg("amount").alias("avg_transaction"),
            countDistinct("cardholder_id").alias("unique_customers"),
            countDistinct("merchant_name").alias("unique_merchants_visited"),
            sum(when(col("is_weekend"), 1).otherwise(0)).alias("weekend_transactions"),
            sum(when(col("is_lunch_hour"), 1).otherwise(0)).alias("lunch_transactions"),
            sum(when(col("is_dinner_hour"), 1).otherwise(0)).alias("dinner_transactions")
        )
        .withColumn("avg_spend_per_customer", col("total_spend") / col("unique_customers"))
        .withColumn("transactions_per_customer", col("transaction_count") / col("unique_customers"))
        .withColumn("weekend_ratio", col("weekend_transactions") / col("transaction_count"))
    )
