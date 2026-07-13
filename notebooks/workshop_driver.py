# Databricks notebook source
# MAGIC %md
# MAGIC # Illumia Campus Analytics Workshop
# MAGIC
# MAGIC Build a complete analytics platform for campus operations:
# MAGIC - **Data Ingestion** - Autoloader from 4 source systems
# MAGIC - **Transformation** - Declarative Pipelines (Bronze - Silver - Gold)
# MAGIC - **Visualization** - Lakeview Dashboard
# MAGIC - **AI Assistant** - Embedded Genie Agent
# MAGIC - **Deployment** - Databricks App
# MAGIC
# MAGIC **AWS Equivalent**: S3 + Glue + Step Functions + QuickSight + API Gateway + Lambda + Bedrock
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Workshop Steps
# MAGIC
# MAGIC | Step | Title |
# MAGIC |------|-------|
# MAGIC | 1 | Copy Git Repo to Personal Folder |
# MAGIC | 2 | Create Catalog/Schema/Volume |
# MAGIC | 3 | Run Data Generation via Lakeflow Job |
# MAGIC | 4 | Explore Generated Data |
# MAGIC | 5 | Create Declarative Pipeline |
# MAGIC | 6 | Explore Gold Tables |
# MAGIC | 7 | Create Dashboard | 
# MAGIC | 8 | Setup Genie Agent |
# MAGIC | 9 | Deploy App |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Environment Setup
# MAGIC
# MAGIC Derive your participant ID and set up configuration variables.

# COMMAND ----------

dbutils.widgets.text("catalog", "illumia_demo_catalog")
dbutils.widgets.text("schema", "workshop_data")

# COMMAND ----------

# Derive participant ID from username
user_email = spark.sql("SELECT current_user()").first()[0]
USER_ID = user_email.split("@")[0].replace(".", "_").replace("-", "_")
USER_EMAIL = user_email

USER_CATALOG = dbutils.widgets.get("catalog")
USER_SCHEMA = dbutils.widgets.get("schema")

print(f"Welcome, {USER_ID}!")
print(f"Email: {USER_EMAIL}")
print(f"")
print(f"Your resources will be created in: {USER_CATALOG}.{USER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Create GitHub Folder
# MAGIC
# MAGIC Each participant gets their own copy of the workshop assets.

# COMMAND ----------

WORKSHOP_PATH = "" # fill in with github folder path

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Create Your Personal Namespace
# MAGIC
# MAGIC Each participant creates their own schema within the pre-provisioned catalog.

# COMMAND ----------

# Create personal schema (catalog must already exist)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {USER_CATALOG}.{USER_SCHEMA}")

# Create checkpoint volume for pipeline state
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {USER_CATALOG}.{USER_SCHEMA}.checkpoints
""")

# Set context
spark.sql(f"USE CATALOG {USER_CATALOG}")
spark.sql(f"USE SCHEMA {USER_SCHEMA}")

CHECKPOINT_PATH = f"/Volumes/{USER_CATALOG}/{USER_SCHEMA}/checkpoints"

print(f"Created schema: {USER_CATALOG}.{USER_SCHEMA}")
print(f"Created volume: {CHECKPOINT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Run Data Generation via Lakeflow Job
# MAGIC
# MAGIC Create a Lakeflow Job to run the data generation notebook. This approach is production-ready.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask, Source

w = WorkspaceClient()

# Path to data generation notebook
notebook_path = f"{WORKSHOP_PATH}/notebooks/01_generate_data"

# Create job with parameters for catalog and schema
job = w.jobs.create(
    name=f"{USER_ID}_illumia_data_generation",
    tasks=[
        Task(
            task_key="generate_data",
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                source=Source.WORKSPACE,
                base_parameters={
                    "catalog": USER_CATALOG,
                    "schema": USER_SCHEMA
                }
            ),
            existing_cluster_id= # fill in with shared cluster ID
        )
    ]
)

print(f"Created job: {job.job_id}")
print(f"  Name: {USER_ID}_illumia_data_generation")

# Run the job
run = w.jobs.run_now(job_id=job.job_id)
print(f"")
print(f"Started run: {run.run_id}")
print(f"Monitor at: {w.config.host}#job/{job.job_id}/run/{run.run_id}")

# COMMAND ----------

# Wait for job completion
import time

print("Waiting for data generation to complete...")
print("")

while True:
    run_status = w.jobs.get_run(run_id=run.run_id)
    state = run_status.state.life_cycle_state.value
    print(f"  Status: {state}")

    if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        result = run_status.state.result_state.value if run_status.state.result_state else "UNKNOWN"
        print(f"")
        print(f"Final result: {result}")
        break

    time.sleep(10)

print("")
print("Data generation complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Explore the Generated Data
# MAGIC
# MAGIC Four source systems representing the Illumia campus platform:
# MAGIC - **Cardholders**: Student/staff identity (CS Gold)
# MAGIC - **Transactions**: POS purchases (GET platform)
# MAGIC - **Access Events**: Building entry/exit (CS Access)
# MAGIC - **Food Service**: Dining operations (NetMenu)

# COMMAND ----------

VOLUME_BASE = f"/Volumes/{USER_CATALOG}/{USER_SCHEMA}"

# Preview each data source
sources = {
    "cardholders": f"{VOLUME_BASE}/cardholders/",
    "transactions": f"{VOLUME_BASE}/transactions/",
    "access_events": f"{VOLUME_BASE}/access_events/",
    "food_service": f"{VOLUME_BASE}/food_service/"
}

for name, path in sources.items():
    print(f"\n{'='*60}")
    print(f"{name.upper()}")
    print(f"{'='*60}")
    df = spark.read.json(path)
    print(f"Records: {df.count():,}")
    print(f"Columns: {', '.join(df.columns[:8])}...")

# COMMAND ----------

# Explore cardholder distribution
print("Cardholder breakdown by patron type and status:")
display(
    spark.read.json(f"{VOLUME_BASE}/cardholders/")
    .groupBy("patron_type", "status")
    .count()
    .orderBy("patron_type", "status")
)

# COMMAND ----------

# Transaction patterns by merchant category
from pyspark.sql.functions import col, sum, avg, count

print("Transaction summary by merchant category:")
display(
    spark.read.json(f"{VOLUME_BASE}/transactions/")
    .groupBy("merchant_category")
    .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_transaction")
    )
    .orderBy(col("total_revenue").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Create the Declarative Pipeline
# MAGIC
# MAGIC Declarative Pipelines (DP) automate the medallion architecture:
# MAGIC - **Bronze**: Raw ingestion with Autoloader
# MAGIC - **Silver**: Cleaned, validated, enriched with expectations
# MAGIC - **Gold**: Business aggregations for analytics

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary

w = WorkspaceClient()

# Path to the declarative pipeline notebook
pipeline_notebook = f"{WORKSHOP_PATH}/notebooks/02_declarative_pipeline"

# Create the pipeline
pipeline = w.pipelines.create(
    name=f"{USER_ID}_illumia_pipeline",
    catalog=USER_CATALOG,
    target=USER_SCHEMA,
    libraries=[
        PipelineLibrary(
            notebook=NotebookLibrary(path=pipeline_notebook)
        )
    ],
    configuration={
        "pipeline.catalog": USER_CATALOG,
        "pipeline.schema": USER_SCHEMA
    },
    serverless=True,
    continuous=False
)

print(f"Created pipeline: {pipeline.pipeline_id}")
print(f"  Name: {USER_ID}_illumia_pipeline")
print(f"  Target: {USER_CATALOG}.{USER_SCHEMA}")

# COMMAND ----------

# Start the pipeline
update = w.pipelines.start_update(pipeline_id=pipeline.pipeline_id)
print(f"Started pipeline update: {update.update_id}")
print(f"")
print(f"Monitor at: {w.config.host}#joblist/pipelines/{pipeline.pipeline_id}")

# COMMAND ----------

# Wait for pipeline completion
import time

print("Waiting for pipeline to complete...")
print("")

while True:
    status = w.pipelines.get(pipeline_id=pipeline.pipeline_id)
    state = status.latest_updates[0].state.value if status.latest_updates else "UNKNOWN"
    print(f"  State: {state}")

    if state in ["COMPLETED", "FAILED", "CANCELED"]:
        break

    time.sleep(15)

print("")
print("Pipeline complete!")
print("")
print("Tables created:")
for table in spark.catalog.listTables(f"{USER_CATALOG}.{USER_SCHEMA}"):
    print(f"  - {table.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Explore Gold Tables
# MAGIC
# MAGIC The gold layer contains business-ready aggregations:
# MAGIC - **gold_cardholder_360**: Unified view of each cardholder
# MAGIC - **gold_location_analytics**: Revenue by location and time
# MAGIC - **gold_dining_operations**: Food waste and efficiency
# MAGIC - **gold_behavior_patterns**: Cross-domain insights

# COMMAND ----------

# Top engaged cardholders by patron type (using window function)
print("Top 5 engaged cardholders by patron type:")
display(spark.sql(f"""
SELECT
    patron_type_clean,
    rank,
    cardholder_id,
    engagement_score,
    total_spend,
    total_transactions,
    unique_buildings
FROM (
    SELECT
        patron_type_clean,
        cardholder_id,
        engagement_score,
        total_spend,
        total_transactions,
        unique_buildings,
        ROW_NUMBER() OVER (PARTITION BY patron_type_clean ORDER BY engagement_score DESC) as rank
    FROM {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
)
WHERE rank <= 5
ORDER BY patron_type_clean, rank
"""))

# COMMAND ----------

# Revenue trends with day-over-day change (using LAG window function)
print("Location revenue with day-over-day trends:")
display(spark.sql(f"""
SELECT
    merchant_name,
    transaction_date,
    total_revenue,
    revenue_change,
    ROUND(change_pct, 1) as change_pct,
    unique_customers
FROM (
    SELECT
        merchant_name,
        transaction_date,
        total_revenue,
        unique_customers,
        total_revenue - LAG(total_revenue) OVER (PARTITION BY merchant_name ORDER BY transaction_date) as revenue_change,
        ((total_revenue - LAG(total_revenue) OVER (PARTITION BY merchant_name ORDER BY transaction_date))
            / LAG(total_revenue) OVER (PARTITION BY merchant_name ORDER BY transaction_date) * 100) as change_pct
    FROM {USER_CATALOG}.{USER_SCHEMA}.gold_location_analytics
)
WHERE revenue_change IS NOT NULL
ORDER BY merchant_name, transaction_date
LIMIT 20
"""))

# COMMAND ----------

# Food waste analysis - identify optimization opportunities
print("Food waste analysis - optimization opportunities:")
display(spark.sql(f"""
SELECT
    location_name,
    ROUND(AVG(waste_percentage), 1) as avg_waste_pct,
    SUM(total_wasted) as total_portions_wasted,
    ROUND(AVG(efficiency_rate), 1) as avg_efficiency,
    ROUND(SUM(total_food_cost), 2) as total_cost,
    ROUND(SUM(total_food_cost) * (AVG(waste_percentage) / 100), 2) as potential_savings
FROM {USER_CATALOG}.{USER_SCHEMA}.gold_dining_operations
GROUP BY location_name
ORDER BY avg_waste_pct DESC
"""))

# COMMAND ----------

# Cross-domain insight: Spending patterns by housing area
print("Cross-domain insight: Spending patterns by housing area")
display(spark.sql(f"""
SELECT *
FROM {USER_CATALOG}.{USER_SCHEMA}.gold_behavior_patterns
ORDER BY avg_spend_per_customer DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Create the Lakeview Dashboard
# MAGIC
# MAGIC Create a dashboard with 4 widgets from our gold tables:
# MAGIC 1. **Revenue by Location** - gold_location_analytics
# MAGIC 2. **Food Waste by Hall** - gold_dining_operations
# MAGIC 3. **Engagement Distribution** - gold_cardholder_360
# MAGIC 4. **Spending by Housing** - gold_behavior_patterns
# MAGIC
# MAGIC First, we'll define the queries, then create the dashboard programmatically using the SDK.

# COMMAND ----------

# Widget 1: Revenue by Location (Bar Chart)
query1 = f"""
SELECT
    merchant_name,
    merchant_category,
    SUM(total_revenue) AS total_revenue,
    SUM(transaction_count) AS total_transactions,
    ROUND(AVG(avg_transaction), 2) AS avg_transaction_value
FROM {USER_CATALOG}.{USER_SCHEMA}.gold_location_analytics
GROUP BY merchant_name, merchant_category
ORDER BY total_revenue DESC
LIMIT 15
"""
print("WIDGET 1: Revenue by Location (Bar Chart)")
print("="*60)
print(query1)
display(spark.sql(query1))

# COMMAND ----------

# Widget 2: Food Waste by Dining Hall (Grouped Bar Chart)
query2 = f"""
SELECT
    location_name,
    meal_period,
    ROUND(AVG(waste_percentage), 1) AS avg_waste_pct,
    SUM(total_wasted) AS total_portions_wasted,
    ROUND(AVG(efficiency_rate), 1) AS avg_efficiency_rate
FROM {USER_CATALOG}.{USER_SCHEMA}.gold_dining_operations
GROUP BY location_name, meal_period
ORDER BY location_name, meal_period
"""
print("WIDGET 2: Food Waste by Dining Hall (Grouped Bar Chart)")
print("="*60)
print(query2)
display(spark.sql(query2))

# COMMAND ----------

# Widget 3: Cardholder Engagement Distribution (Pie Chart)
query3 = f"""
SELECT
    engagement_tier,
    patron_type_clean AS patron_type,
    COUNT(*) AS cardholder_count,
    ROUND(AVG(engagement_score), 1) AS avg_engagement_score,
    ROUND(AVG(total_spend), 2) AS avg_total_spend
FROM {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
WHERE is_active = true
GROUP BY engagement_tier, patron_type_clean
ORDER BY engagement_tier, cardholder_count DESC
"""
print("WIDGET 3: Engagement Distribution (Pie Chart)")
print("="*60)
print(query3)
display(spark.sql(query3))

# COMMAND ----------

# Widget 4: Spending by Housing Area (Bar Chart)
query4 = f"""
SELECT
    housing_area,
    patron_type_clean AS patron_type,
    transaction_count,
    ROUND(total_spend, 2) AS total_spend,
    ROUND(avg_spend_per_customer, 2) AS avg_spend_per_customer,
    ROUND(weekend_ratio * 100, 1) AS weekend_pct
FROM {USER_CATALOG}.{USER_SCHEMA}.gold_behavior_patterns
WHERE housing_area IS NOT NULL
ORDER BY avg_spend_per_customer DESC
"""
print("WIDGET 4: Spending by Housing Area (Bar Chart)")
print("="*60)
print(query4)
display(spark.sql(query4))

# COMMAND ----------

# Create the dashboard using the Lakeview API
import json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

dashboard_name = f"Illumia Analytics - {USER_ID}"

# Define the dashboard with datasets and widgets
dashboard_definition = {
    "datasets": [
        {
            "name": "revenue_by_location",
            "displayName": "Revenue by Location",
            "query": query1.strip()
        },
        {
            "name": "food_waste",
            "displayName": "Food Waste by Dining Hall",
            "query": query2.strip()
        },
        {
            "name": "engagement_distribution",
            "displayName": "Engagement Distribution",
            "query": query3.strip()
        },
        {
            "name": "spending_by_housing",
            "displayName": "Spending by Housing Area",
            "query": query4.strip()
        }
    ],
    "pages": [
        {
            "name": "main",
            "displayName": "Campus Analytics",
            "layout": [
                {
                    "widget": {
                        "name": "revenue_chart",
                        "queries": [{"name": "revenue_by_location", "query": {"datasetName": "revenue_by_location"}}],
                        "spec": {
                            "version": 2,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "merchant_name", "displayName": "Merchant"},
                                "y": {"fieldName": "total_revenue", "displayName": "Total Revenue"}
                            }
                        }
                    },
                    "position": {"x": 0, "y": 0, "width": 3, "height": 2}
                },
                {
                    "widget": {
                        "name": "waste_chart",
                        "queries": [{"name": "food_waste", "query": {"datasetName": "food_waste"}}],
                        "spec": {
                            "version": 2,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "location_name", "displayName": "Location"},
                                "y": {"fieldName": "avg_waste_pct", "displayName": "Avg Waste %"},
                                "color": {"fieldName": "meal_period", "displayName": "Meal Period"}
                            }
                        }
                    },
                    "position": {"x": 3, "y": 0, "width": 3, "height": 2}
                },
                {
                    "widget": {
                        "name": "engagement_chart",
                        "queries": [{"name": "engagement_distribution", "query": {"datasetName": "engagement_distribution"}}],
                        "spec": {
                            "version": 2,
                            "widgetType": "pie",
                            "encodings": {
                                "theta": {"fieldName": "cardholder_count", "displayName": "Count"},
                                "color": {"fieldName": "engagement_tier", "displayName": "Engagement Tier"}
                            }
                        }
                    },
                    "position": {"x": 0, "y": 2, "width": 3, "height": 2}
                },
                {
                    "widget": {
                        "name": "housing_chart",
                        "queries": [{"name": "spending_by_housing", "query": {"datasetName": "spending_by_housing"}}],
                        "spec": {
                            "version": 2,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "housing_area", "displayName": "Housing Area"},
                                "y": {"fieldName": "avg_spend_per_customer", "displayName": "Avg Spend per Customer"}
                            }
                        }
                    },
                    "position": {"x": 3, "y": 2, "width": 3, "height": 2}
                }
            ]
        }
    ]
}

# Create the dashboard
dashboard = w.lakeview.create(
    display_name=dashboard_name,
    serialized_dashboard=json.dumps(dashboard_definition),
    parent_path=f"/Workspace/Users/{USER_EMAIL}"
)

DASHBOARD_ID = dashboard.dashboard_id
print(f"Created dashboard: {dashboard_name}")
print(f"Dashboard ID: {DASHBOARD_ID}")
print(f"")
print(f"View at: {w.config.host}sql/dashboardsv3/{DASHBOARD_ID}")

# COMMAND ----------

# Publish the dashboard
w.lakeview.publish(dashboard_id=DASHBOARD_ID)
print(f"Published dashboard: {DASHBOARD_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Setup Genie Agent
# MAGIC
# MAGIC Create a Genie Space with access to your gold tables. The Genie Agent will power the AI assistant in our app.
# MAGIC
# MAGIC ### Instructions
# MAGIC 1. Go to **Genie** in left sidebar (or search "Genie")
# MAGIC 2. Click **Create Genie Space**
# MAGIC 3. Name: `Illumia Analytics - {USER_ID}`
# MAGIC 4. Add the 4 gold tables listed below
# MAGIC 5. Set a sample question: "What are the top 5 dining locations by revenue?"
# MAGIC 6. **Create** the space
# MAGIC 7. Copy the **Genie Space ID** from the URL

# COMMAND ----------

# Tables to add to your Genie Space
gold_tables = [
    f"{USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360",
    f"{USER_CATALOG}.{USER_SCHEMA}.gold_location_analytics",
    f"{USER_CATALOG}.{USER_SCHEMA}.gold_dining_operations",
    f"{USER_CATALOG}.{USER_SCHEMA}.gold_behavior_patterns",
]

print("Add these tables to your Genie Space:")
print("="*60)
for table in gold_tables:
    print(f"  {table}")

print("")
print("Sample questions to test:")
print("  - What are the top 5 dining locations by revenue?")
print("  - Which housing area has the highest average spend per customer?")
print("  - What is the average food waste percentage by dining hall?")
print("  - Show me cardholders with high engagement scores")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 9: Deploy the Databricks App
# MAGIC
# MAGIC Deploy the app with embedded dashboard and Genie Agent chat interface.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Dashboard ID (already set from Step 7)
# MAGIC - Genie Space ID from Step 8

# COMMAND ----------

# DASHBOARD_ID was set in Step 7 when we created the dashboard
# IMPORTANT: Replace GENIE_SPACE_ID with your actual ID from Step 8
GENIE_SPACE_ID = "YOUR_GENIE_SPACE_ID_HERE"  # e.g., "01f17eb9d71413c99a1aa2e5716ddf23"

print("Configuration for app deployment:")
print("="*60)
print(f"  DASHBOARD_ID:   {DASHBOARD_ID}")
print(f"  GENIE_SPACE_ID: {GENIE_SPACE_ID}")
print("")
if "YOUR_" in GENIE_SPACE_ID:
    print("WARNING: Please replace GENIE_SPACE_ID with your actual ID from Step 8!")

# COMMAND ----------

# Generate the app.yaml configuration
app_yaml_content = f"""command:
  - uvicorn
  - server.main:app
  - --host=0.0.0.0
  - --port=8000

env:
  - name: GENIE_SPACE_ID
    value: "{GENIE_SPACE_ID}"
  - name: DASHBOARD_ID
    value: "{DASHBOARD_ID}"
"""

print("app.yaml configuration:")
print("="*60)
print(app_yaml_content)

# Write to the app folder
app_yaml_path = f"{WORKSHOP_PATH}/app/app.yaml"

with open(app_yaml_path.replace("/Workspace", "/Workspace"), "w") as f:
    f.write(app_yaml_content)

print(f"Updated: {app_yaml_path}")

# COMMAND ----------

# Deploy the app
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

app_name = f"illumia-{USER_ID.replace('_', '-')}"
app_path = f"{WORKSHOP_PATH}/app"

print(f"Deploying app: {app_name}")
print(f"Source: {app_path}")
print("")

# Create the app
try:
    app = w.apps.create_and_wait(
        name=app_name,
        description="Illumia Campus Analytics with Genie Agent"
    )
    print(f"Created app: {app.name}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"App {app_name} already exists, will update deployment")
        app = w.apps.get(name=app_name)
    else:
        raise e

# Deploy the code
print("")
print("Deploying code...")
deployment = w.apps.deploy_and_wait(
    app_name=app_name,
    source_code_path=app_path
)

print(f"Deployment status: {deployment.status}")
print("")
print(f"Open your app at: https://{w.config.host.replace('https://', '')}/apps/{app_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Workshop Complete!
# MAGIC
# MAGIC ### What We Built
# MAGIC
# MAGIC | Component | Databricks Feature |
# MAGIC |-----------|-------------------|
# MAGIC | Data Ingestion | Autoloader |
# MAGIC | Transformation | Declarative Pipeline |
# MAGIC | Data Quality | DP Expectations |
# MAGIC | Analytics | Lakeview Dashboard |
# MAGIC | AI Assistant | Genie Agent |
# MAGIC | App | Databricks App |
# MAGIC
# MAGIC ### Your Resources
# MAGIC
# MAGIC Run the cell below to see a summary of everything created.

# COMMAND ----------

print("="*60)
print("WORKSHOP SUMMARY")
print("="*60)
print("")
print(f"Catalog:   {USER_CATALOG}")
print(f"Schema:    {USER_CATALOG}.{USER_SCHEMA}")
print(f"Pipeline:  {USER_ID}_illumia_pipeline")
print(f"Job:       {USER_ID}_illumia_data_generation")
print(f"App:       illumia-{USER_ID.replace('_', '-')}")
print("")
print("Tables created:")
for table in spark.catalog.listTables(f"{USER_CATALOG}.{USER_SCHEMA}"):
    print(f"  - {table.name}")
print("")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup (Optional)
# MAGIC
# MAGIC Run the cell below to delete all resources created during this workshop.

# COMMAND ----------

# UNCOMMENT TO CLEAN UP ALL RESOURCES
#
# from databricks.sdk import WorkspaceClient
# w = WorkspaceClient()
#
# # Delete pipeline
# try:
#     pipelines = list(w.pipelines.list_pipelines(filter=f"name LIKE '{USER_ID}_illumia_pipeline'"))
#     for p in pipelines:
#         w.pipelines.delete(pipeline_id=p.pipeline_id)
#         print(f"Deleted pipeline: {p.name}")
# except Exception as e:
#     print(f"Pipeline cleanup: {e}")
#
# # Delete job
# try:
#     jobs = list(w.jobs.list(name=f"{USER_ID}_illumia_data_generation"))
#     for j in jobs:
#         w.jobs.delete(job_id=j.job_id)
#         print(f"Deleted job: {j.settings.name}")
# except Exception as e:
#     print(f"Job cleanup: {e}")
#
# # Delete app
# try:
#     w.apps.delete(name=f"illumia-{USER_ID.replace('_', '-')}")
#     print(f"Deleted app: illumia-{USER_ID.replace('_', '-')}")
# except Exception as e:
#     print(f"App cleanup: {e}")
#
# # Delete schema (this drops all tables)
# try:
#     spark.sql(f"DROP SCHEMA IF EXISTS {USER_CATALOG}.{USER_SCHEMA} CASCADE")
#     print(f"Deleted schema: {USER_CATALOG}.{USER_SCHEMA}")
# except Exception as e:
#     print(f"Schema cleanup: {e}")
#
# print("")
# print("Cleanup complete!")
