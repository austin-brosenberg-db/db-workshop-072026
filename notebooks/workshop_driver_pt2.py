# Databricks notebook source
# MAGIC %md
# MAGIC # Illumia Campus Analytics Workshop - Part 2: Advanced Topics
# MAGIC
# MAGIC Build on the foundation from Part 1 with production-ready capabilities:
# MAGIC - **Advanced Data Engineering** - Liquid Clustering, Change Data Feed, Time Travel
# MAGIC - **Pipeline Architecture** - When to use SDP vs Notebook Tasks
# MAGIC - **Security** - Row-Level Filtering, Column Masking, Encryption with Secrets
# MAGIC - **CI/CD** - Databricks Asset Bundles for production deployment
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC This notebook assumes you have completed Part 1 and have:
# MAGIC - Gold tables created in your schema
# MAGIC - A working pipeline and dashboard
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Sections
# MAGIC
# MAGIC | Section | Topic |
# MAGIC |---------|-------|
# MAGIC | 1 | Advanced Data Engineering Capabilities |
# MAGIC | 2 | SDP vs Notebook Tasks in Lakeflow Jobs |
# MAGIC | 3 | Security: CLM, RLF, and Encryption |
# MAGIC | 4 | CI/CD with Databricks Asset Bundles |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Load Configuration from Part 1

# COMMAND ----------

dbutils.widgets.text("catalog", "illumia_demo_catalog")
dbutils.widgets.text("schema", "workshop_data")

USER_CATALOG = dbutils.widgets.get("catalog")
USER_SCHEMA = dbutils.widgets.get("schema")

user_email = spark.sql("SELECT current_user()").first()[0]
USER_ID = user_email.split("@")[0].replace(".", "_").replace("-", "_")
USER_EMAIL = user_email

spark.sql(f"USE CATALOG {USER_CATALOG}")
spark.sql(f"USE SCHEMA {USER_SCHEMA}")

print(f"Welcome back, {USER_ID}!")
print(f"Working with: {USER_CATALOG}.{USER_SCHEMA}")

# COMMAND ----------

# Verify gold tables exist
gold_tables = ["gold_cardholder_360", "gold_location_analytics", "gold_dining_operations", "gold_behavior_patterns"]
existing_tables = [t.name for t in spark.catalog.listTables()]

missing = [t for t in gold_tables if t not in existing_tables]
if missing:
    print(f"WARNING: Missing tables from Part 1: {missing}")
    print("Please run Part 1 first!")
else:
    print("All gold tables found. Ready to proceed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 1: Advanced Data Engineering Capabilities
# MAGIC
# MAGIC Delta Lake provides powerful features for optimizing performance and enabling advanced analytics patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Liquid Clustering
# MAGIC
# MAGIC Liquid Clustering is the modern replacement for Z-ORDER. It automatically manages data layout
# MAGIC for optimal query performance without manual maintenance.
# MAGIC
# MAGIC **Benefits over Z-ORDER:**
# MAGIC - No manual OPTIMIZE commands needed
# MAGIC - Adapts to changing query patterns
# MAGIC - Works incrementally on new data
# MAGIC - Supports multiple clustering columns

# COMMAND ----------

# Create a copy of gold_cardholder_360 to demonstrate liquid clustering
# (We don't want to modify the original table)

spark.sql(f"""
    CREATE OR REPLACE TABLE {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360_clustered
    CLUSTER BY (cardholder_id, patron_type_clean)
    AS SELECT * FROM {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
""")

print("Created clustered table: gold_cardholder_360_clustered")
print("Clustered by: cardholder_id, patron_type_clean")

# COMMAND ----------

# Verify clustering configuration
display(spark.sql(f"""
    DESCRIBE DETAIL {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360_clustered
"""))

# COMMAND ----------

# Compare query performance: clustered vs non-clustered
# Query 1: Point lookup on cardholder_id (benefits from clustering)
import time

cardholder_sample = spark.sql(f"""
    SELECT cardholder_id FROM {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360 LIMIT 1
""").first()[0]

# Time query on original table
start = time.time()
spark.sql(f"""
    SELECT * FROM {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
    WHERE cardholder_id = '{cardholder_sample}'
""").collect()
original_time = time.time() - start

# Time query on clustered table
start = time.time()
spark.sql(f"""
    SELECT * FROM {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360_clustered
    WHERE cardholder_id = '{cardholder_sample}'
""").collect()
clustered_time = time.time() - start

print(f"Point lookup performance comparison:")
print(f"  Original table:  {original_time:.3f}s")
print(f"  Clustered table: {clustered_time:.3f}s")
print("")
print("Note: Performance difference is more pronounced with larger datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Change Data Feed (CDF)
# MAGIC
# MAGIC Change Data Feed tracks row-level changes (INSERT, UPDATE, DELETE) to Delta tables.
# MAGIC This enables:
# MAGIC - Incremental ETL pipelines
# MAGIC - Audit trails
# MAGIC - Real-time synchronization
# MAGIC - CDC (Change Data Capture) patterns

# COMMAND ----------

# Enable CDF on a gold table
spark.sql(f"""
    ALTER TABLE {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

print("Enabled Change Data Feed on gold_cardholder_360")

# COMMAND ----------

# Make some changes to demonstrate CDF
# First, let's update some engagement tiers

spark.sql(f"""
    UPDATE {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
    SET engagement_tier = 'Platinum',
        engagement_score = engagement_score + 10
    WHERE engagement_tier = 'Gold' AND engagement_score > 70
    LIMIT 5
""")

print("Updated 5 cardholders from Gold to Platinum tier")

# COMMAND ----------

# Query the change data feed to see what changed
print("Recent changes captured by Change Data Feed:")
display(spark.sql(f"""
    SELECT
        cardholder_id,
        cardholder_name,
        engagement_tier,
        engagement_score,
        _change_type,
        _commit_version,
        _commit_timestamp
    FROM table_changes('{USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360', 1)
    ORDER BY _commit_timestamp DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDF Use Case: Audit Trail
# MAGIC
# MAGIC Track all tier changes for compliance reporting.

# COMMAND ----------

# Create an audit view showing tier progression
print("Engagement tier change audit trail:")
display(spark.sql(f"""
    SELECT
        cardholder_id,
        _change_type as change_type,
        engagement_tier,
        engagement_score,
        _commit_timestamp as changed_at,
        _commit_version as version
    FROM table_changes('{USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360', 1)
    WHERE _change_type IN ('update_preimage', 'update_postimage')
    ORDER BY cardholder_id, _commit_timestamp
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Time Travel
# MAGIC
# MAGIC Delta Lake automatically versions data, enabling:
# MAGIC - Query historical snapshots
# MAGIC - Audit data as of specific timestamps
# MAGIC - Recover from accidental changes
# MAGIC - Reproduce past analyses

# COMMAND ----------

# View table history
print("Table version history:")
display(spark.sql(f"""
    DESCRIBE HISTORY {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
    LIMIT 10
"""))

# COMMAND ----------

# Query data as it existed before our updates (version 0 or 1)
print("Data as of the first version (before our updates):")
display(spark.sql(f"""
    SELECT
        cardholder_id,
        cardholder_name,
        engagement_tier,
        engagement_score
    FROM {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360 VERSION AS OF 1
    WHERE engagement_tier = 'Gold'
    LIMIT 10
"""))

# COMMAND ----------

# Compare current vs historical state
print("Comparing current vs historical engagement tier distribution:")
display(spark.sql(f"""
    SELECT
        'Current' as snapshot,
        engagement_tier,
        COUNT(*) as count
    FROM {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
    GROUP BY engagement_tier

    UNION ALL

    SELECT
        'Version 1' as snapshot,
        engagement_tier,
        COUNT(*) as count
    FROM {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360 VERSION AS OF 1
    GROUP BY engagement_tier

    ORDER BY snapshot, engagement_tier
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel Use Case: Point-in-Time Reporting
# MAGIC
# MAGIC "What was this student's spend last week before the correction?"

# COMMAND ----------

# Query by timestamp (adjust timestamp as needed)
from datetime import datetime, timedelta

# Get a timestamp from the history
history = spark.sql(f"""
    SELECT timestamp FROM (DESCRIBE HISTORY {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360)
    ORDER BY version DESC LIMIT 1
""").first()[0]

print(f"Querying data as of: {history}")
display(spark.sql(f"""
    SELECT
        cardholder_id,
        total_spend,
        total_transactions,
        engagement_score
    FROM {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
    TIMESTAMP AS OF '{history}'
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 OPTIMIZE and VACUUM
# MAGIC
# MAGIC Table maintenance commands for optimal performance and storage management.
# MAGIC
# MAGIC - **OPTIMIZE**: Compacts small files into larger ones for better read performance
# MAGIC - **VACUUM**: Removes old file versions to reclaim storage

# COMMAND ----------

# Run OPTIMIZE to compact files
print("Running OPTIMIZE on gold_location_analytics...")
spark.sql(f"""
    OPTIMIZE {USER_CATALOG}.{USER_SCHEMA}.gold_location_analytics
""")
print("OPTIMIZE complete!")

# COMMAND ----------

# Check file statistics after optimization
display(spark.sql(f"""
    DESCRIBE DETAIL {USER_CATALOG}.{USER_SCHEMA}.gold_location_analytics
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM Safety Considerations
# MAGIC
# MAGIC **Important**: VACUUM removes old file versions permanently!
# MAGIC
# MAGIC - Default retention is 7 days
# MAGIC - Cannot time travel to versions older than retention period after VACUUM
# MAGIC - Production recommendation: Keep at least 7 days for recovery

# COMMAND ----------

# Check what files would be vacuumed (dry run)
print("Files eligible for cleanup (DRY RUN - nothing deleted):")
display(spark.sql(f"""
    VACUUM {USER_CATALOG}.{USER_SCHEMA}.gold_location_analytics DRY RUN
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 2: SDP vs Notebook Tasks in Lakeflow Jobs
# MAGIC
# MAGIC Understanding when to use **Spark Declarative Pipelines (SDP)** versus **Notebook Tasks**
# MAGIC in Lakeflow Jobs is critical for building maintainable, cost-effective data platforms.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decision Framework
# MAGIC
# MAGIC ### Use Spark Declarative Pipelines (SDP) When:
# MAGIC
# MAGIC | Scenario | Why SDP |
# MAGIC |----------|---------|
# MAGIC | **Multi-stage medallion architecture** | SDP manages DAG dependencies automatically |
# MAGIC | **Incremental processing** | Built-in incremental refresh eliminates manual state management |
# MAGIC | **Data quality enforcement** | Expectations framework provides declarative quality checks |
# MAGIC | **Lineage requirements** | Automatic lineage tracking in Unity Catalog |
# MAGIC | **Streaming ingestion** | Native streaming table support with exactly-once semantics |
# MAGIC | **Team collaboration** | Declarative SQL/Python is easier to review and maintain |
# MAGIC
# MAGIC ### Use Notebook Tasks in Lakeflow Jobs When:
# MAGIC
# MAGIC | Scenario | Why Notebooks |
# MAGIC |----------|---------------|
# MAGIC | **Exploratory analysis** | Cell-by-cell execution, visualizations, iteration |
# MAGIC | **ML pipelines** | MLflow integration, model training, hyperparameter tuning |
# MAGIC | **Complex control flow** | Conditional logic, loops, external API calls |
# MAGIC | **Heavy side effects** | REST APIs, notifications, file operations |
# MAGIC | **One-off data fixes** | Backfills, migrations, data corrections |
# MAGIC | **Custom Spark features** | SparkListeners, custom metrics, advanced configurations |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison: The Same Transformation Two Ways
# MAGIC
# MAGIC Let's see how the same business logic looks in both approaches.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Approach 1: Spark Declarative Pipeline (SDP)
# MAGIC
# MAGIC ```python
# MAGIC # In a declarative pipeline notebook
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import col, sum, avg
# MAGIC
# MAGIC @dlt.table(
# MAGIC     comment="Daily revenue aggregation by merchant",
# MAGIC     table_properties={"quality": "gold"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_revenue", "total_revenue > 0")
# MAGIC def gold_daily_revenue():
# MAGIC     return (
# MAGIC         dlt.read("silver_transactions")
# MAGIC         .groupBy("merchant_name", "transaction_date")
# MAGIC         .agg(
# MAGIC             sum("amount").alias("total_revenue"),
# MAGIC             avg("amount").alias("avg_transaction")
# MAGIC         )
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Declarative: Define WHAT, not HOW
# MAGIC - Built-in expectations for data quality
# MAGIC - Automatic dependency resolution
# MAGIC - Managed incremental refresh

# COMMAND ----------

# MAGIC %md
# MAGIC ### Approach 2: Notebook Task in Lakeflow Job
# MAGIC
# MAGIC ```python
# MAGIC # In a regular notebook scheduled as a Job task
# MAGIC from pyspark.sql.functions import col, sum, avg
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC # Read source with explicit path
# MAGIC silver_df = spark.read.table("catalog.schema.silver_transactions")
# MAGIC
# MAGIC # Transform
# MAGIC gold_df = (
# MAGIC     silver_df
# MAGIC     .groupBy("merchant_name", "transaction_date")
# MAGIC     .agg(
# MAGIC         sum("amount").alias("total_revenue"),
# MAGIC         avg("amount").alias("avg_transaction")
# MAGIC     )
# MAGIC     .filter(col("total_revenue") > 0)  # Manual quality filter
# MAGIC )
# MAGIC
# MAGIC # Write with merge logic for idempotency
# MAGIC target = DeltaTable.forName(spark, "catalog.schema.gold_daily_revenue")
# MAGIC (
# MAGIC     target.alias("t")
# MAGIC     .merge(gold_df.alias("s"), "t.merchant_name = s.merchant_name AND t.transaction_date = s.transaction_date")
# MAGIC     .whenMatchedUpdateAll()
# MAGIC     .whenNotMatchedInsertAll()
# MAGIC     .execute()
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Full control over execution
# MAGIC - Easy debugging and iteration
# MAGIC - Custom merge/upsert logic
# MAGIC - Flexible error handling

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Considerations
# MAGIC
# MAGIC | Factor | SDP | Notebook Tasks |
# MAGIC |--------|-----|----------------|
# MAGIC | **Compute** | Serverless, pay-per-use | Cluster-based or serverless |
# MAGIC | **Idle time** | Minimal (pipelines shut down) | Depends on cluster config |
# MAGIC | **Development** | Lower iteration cost | Higher (cluster spin-up) |
# MAGIC | **Maintenance** | Lower (declarative) | Higher (imperative) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hybrid Approach: Best of Both Worlds
# MAGIC
# MAGIC Many production systems use **both**:
# MAGIC
# MAGIC 1. **SDP** for core data pipelines (Bronze → Silver → Gold)
# MAGIC 2. **Notebook Tasks** for:
# MAGIC    - Data quality reports
# MAGIC    - ML feature engineering
# MAGIC    - External system integrations
# MAGIC    - Alerting and notifications
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
# MAGIC │  Declarative    │     │    Notebook     │     │    Notebook     │
# MAGIC │   Pipeline      │────▶│   ML Training   │────▶│   Send Alerts   │
# MAGIC │  Bronze→Gold    │     │                 │     │   via API       │
# MAGIC └─────────────────┘     └─────────────────┘     └─────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 3: Security - CLM, RLF, and Encryption
# MAGIC
# MAGIC Unity Catalog provides fine-grained access controls to protect sensitive data while
# MAGIC enabling self-service analytics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Row-Level Filtering (RLF)
# MAGIC
# MAGIC Row-Level Filtering restricts which rows a user can see based on their identity or group membership.
# MAGIC
# MAGIC **Use Case**: Dining hall managers should only see data for their own location.

# COMMAND ----------

# First, let's see the current data
print("All dining locations in the data:")
display(spark.sql(f"""
    SELECT DISTINCT location_name
    FROM {USER_CATALOG}.{USER_SCHEMA}.gold_dining_operations
    ORDER BY location_name
"""))

# COMMAND ----------

# Create a row filter function
# This function returns TRUE for rows the user is allowed to see
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {USER_CATALOG}.{USER_SCHEMA}.dining_location_filter(location STRING)
    RETURNS BOOLEAN
    RETURN CASE
        -- Admins see everything
        WHEN is_account_group_member('dining_admins') THEN TRUE
        -- North campus staff see only North Campus Dining
        WHEN current_user() LIKE '%north%' THEN location = 'North Campus Dining'
        -- South campus staff see only South Campus Dining
        WHEN current_user() LIKE '%south%' THEN location = 'South Campus Dining'
        -- Default: allow all for demo purposes (in production, this would be FALSE)
        ELSE TRUE
    END
""")

print("Created row filter function: dining_location_filter")

# COMMAND ----------

# Apply the row filter to the table
spark.sql(f"""
    ALTER TABLE {USER_CATALOG}.{USER_SCHEMA}.gold_dining_operations
    SET ROW FILTER {USER_CATALOG}.{USER_SCHEMA}.dining_location_filter ON (location_name)
""")

print("Applied row filter to gold_dining_operations")
print("Users will now only see rows matching their location permissions")

# COMMAND ----------

# Test the filter (as current user, you'll see all rows due to ELSE TRUE)
print("Rows visible to current user:")
display(spark.sql(f"""
    SELECT location_name, COUNT(*) as visible_rows
    FROM {USER_CATALOG}.{USER_SCHEMA}.gold_dining_operations
    GROUP BY location_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Column-Level Masking (CLM)
# MAGIC
# MAGIC Column-Level Masking hides or transforms sensitive column values based on user permissions.
# MAGIC
# MAGIC **Use Case**: Mask cardholder names for analysts while showing full names to administrators.

# COMMAND ----------

# Create a masking function for cardholder names
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {USER_CATALOG}.{USER_SCHEMA}.mask_cardholder_name(name STRING)
    RETURNS STRING
    RETURN CASE
        -- Admins see full name
        WHEN is_account_group_member('card_admins') THEN name
        -- Others see masked name (first initial + *** + last initial)
        ELSE CONCAT(LEFT(name, 1), '****', RIGHT(name, 1))
    END
""")

print("Created masking function: mask_cardholder_name")

# COMMAND ----------

# Apply the mask to the cardholder_name column
spark.sql(f"""
    ALTER TABLE {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
    ALTER COLUMN cardholder_name SET MASK {USER_CATALOG}.{USER_SCHEMA}.mask_cardholder_name
""")

print("Applied column mask to cardholder_name")

# COMMAND ----------

# Test the mask - non-admins will see masked names
print("Cardholder data with masking applied:")
display(spark.sql(f"""
    SELECT
        cardholder_id,
        cardholder_name,  -- This will be masked for non-admins
        patron_type_clean,
        engagement_tier,
        total_spend
    FROM {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Encryption with Secrets in UDFs
# MAGIC
# MAGIC For highly sensitive data, you can encrypt values at rest and decrypt only when needed
# MAGIC using keys stored in Databricks Secrets.
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
# MAGIC │   Data at   │     │  Encrypted  │     │ Decrypted   │
# MAGIC │    Rest     │────▶│   in Table  │────▶│  via UDF    │
# MAGIC │  (Clear)    │     │  (AES-256)  │     │ (Authorized)│
# MAGIC └─────────────┘     └─────────────┘     └─────────────┘
# MAGIC                           │                    │
# MAGIC                           ▼                    ▼
# MAGIC                     ┌─────────────┐     ┌─────────────┐
# MAGIC                     │   Secret    │     │  User in    │
# MAGIC                     │   Scope     │     │ Auth Group  │
# MAGIC                     └─────────────┘     └─────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Best Practice:** Use Unity Catalog Service Credentials with Azure Key Vault or AWS Secrets
# MAGIC Manager for enterprise deployments. This provides better security than Databricks secret scopes
# MAGIC because workspace admins don't automatically get access to the secrets.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create a Secret Scope (One-time setup)
# MAGIC
# MAGIC Run this in your terminal or via Databricks CLI:
# MAGIC ```bash
# MAGIC # Create a secret scope
# MAGIC databricks secrets create-scope illumia-encryption
# MAGIC
# MAGIC # Add the encryption key (32 bytes for AES-256)
# MAGIC databricks secrets put-secret illumia-encryption encryption-key --string-value "your-32-byte-encryption-key-here"
# MAGIC ```
# MAGIC
# MAGIC For this demo, we'll check if the scope exists and create sample encrypted data.

# COMMAND ----------

# Check if secret scope exists
try:
    scopes = dbutils.secrets.listScopes()
    scope_names = [s.name for s in scopes]
    if "illumia-encryption" in scope_names:
        print("Secret scope 'illumia-encryption' exists")
        ENCRYPTION_AVAILABLE = True
    else:
        print("Secret scope 'illumia-encryption' not found")
        print("To create it, run: databricks secrets create-scope illumia-encryption")
        ENCRYPTION_AVAILABLE = False
except Exception as e:
    print(f"Could not check secret scopes: {e}")
    ENCRYPTION_AVAILABLE = False

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create Encryption/Decryption UDFs
# MAGIC
# MAGIC These UDFs use Databricks' built-in AES encryption functions with keys from secrets.

# COMMAND ----------

# Create a table with encrypted sensitive data
# We'll encrypt the student ID number as an example

spark.sql(f"""
    CREATE OR REPLACE TABLE {USER_CATALOG}.{USER_SCHEMA}.cardholder_sensitive_data AS
    SELECT
        cardholder_id,
        cardholder_name,
        -- Simulate an encrypted student ID using base64 encoding
        -- In production, use AES_ENCRYPT with a secret key
        base64(CAST(cardholder_id AS BINARY)) as encrypted_student_id,
        email,
        patron_type_clean
    FROM {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
    LIMIT 100
""")

print("Created table with simulated encrypted data: cardholder_sensitive_data")

# COMMAND ----------

# Create a decryption UDF that uses secrets
# This pattern checks group membership before decrypting
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {USER_CATALOG}.{USER_SCHEMA}.decrypt_student_id(encrypted_value STRING)
    RETURNS STRING
    RETURN CASE
        -- Only members of 'pii_authorized' group can decrypt
        WHEN is_account_group_member('pii_authorized') THEN
            -- In production: aes_decrypt(unbase64(encrypted_value), secret('scope', 'key'), 'GCM')
            CAST(unbase64(encrypted_value) AS STRING)
        -- Others see a placeholder
        ELSE '[ENCRYPTED]'
    END
""")

print("Created decryption UDF: decrypt_student_id")

# COMMAND ----------

# Create a secure view that applies decryption
spark.sql(f"""
    CREATE OR REPLACE VIEW {USER_CATALOG}.{USER_SCHEMA}.v_cardholder_secure AS
    SELECT
        cardholder_id,
        cardholder_name,
        {USER_CATALOG}.{USER_SCHEMA}.decrypt_student_id(encrypted_student_id) as student_id,
        email,
        patron_type_clean
    FROM {USER_CATALOG}.{USER_SCHEMA}.cardholder_sensitive_data
""")

print("Created secure view: v_cardholder_secure")

# COMMAND ----------

# Query the secure view - unauthorized users see [ENCRYPTED]
print("Secure view output (decryption based on group membership):")
display(spark.sql(f"""
    SELECT * FROM {USER_CATALOG}.{USER_SCHEMA}.v_cardholder_secure
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production Encryption Pattern
# MAGIC
# MAGIC For production use with actual AES encryption:
# MAGIC
# MAGIC ```sql
# MAGIC -- Encryption UDF (for writing data)
# MAGIC CREATE FUNCTION encrypt_pii(value STRING)
# MAGIC RETURNS BINARY
# MAGIC RETURN aes_encrypt(
# MAGIC     value,
# MAGIC     secret('illumia-encryption', 'encryption-key'),
# MAGIC     'GCM'
# MAGIC );
# MAGIC
# MAGIC -- Decryption UDF (for reading data)
# MAGIC CREATE FUNCTION decrypt_pii(encrypted_value BINARY)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC     WHEN is_account_group_member('pii_authorized') THEN
# MAGIC         aes_decrypt(encrypted_value, secret('illumia-encryption', 'encryption-key'), 'GCM')
# MAGIC     ELSE '[ENCRYPTED]'
# MAGIC END;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Data Classification Tags
# MAGIC
# MAGIC Unity Catalog supports tagging columns with sensitivity classifications for compliance reporting.

# COMMAND ----------

# Add classification tags to columns
spark.sql(f"""
    ALTER TABLE {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
    ALTER COLUMN cardholder_name SET TAGS ('pii' = 'true', 'sensitivity' = 'high')
""")

spark.sql(f"""
    ALTER TABLE {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
    ALTER COLUMN email SET TAGS ('pii' = 'true', 'sensitivity' = 'high')
""")

spark.sql(f"""
    ALTER TABLE {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
    ALTER COLUMN total_spend SET TAGS ('pii' = 'false', 'sensitivity' = 'low')
""")

print("Added data classification tags to columns")

# COMMAND ----------

# Query tags for compliance reporting
print("PII columns in gold_cardholder_360:")
display(spark.sql(f"""
    SELECT
        column_name,
        tag_name,
        tag_value
    FROM {USER_CATALOG}.information_schema.column_tags
    WHERE schema_name = '{USER_SCHEMA}'
      AND table_name = 'gold_cardholder_360'
    ORDER BY column_name, tag_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Audit Logging
# MAGIC
# MAGIC Unity Catalog provides system tables for auditing data access patterns.

# COMMAND ----------

# Query audit logs for recent access to our tables
# Note: System tables require appropriate permissions
print("Recent table access (from system.access.audit):")
try:
    display(spark.sql(f"""
        SELECT
            event_time,
            user_identity.email as user_email,
            request_params.full_name_arg as table_name,
            action_name,
            response.status_code
        FROM system.access.audit
        WHERE request_params.full_name_arg LIKE '%{USER_SCHEMA}%'
          AND event_date >= current_date() - 1
        ORDER BY event_time DESC
        LIMIT 20
    """))
except Exception as e:
    print(f"Could not query audit logs: {e}")
    print("System tables require workspace admin or specific grants")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 4: CI/CD with Databricks Asset Bundles
# MAGIC
# MAGIC Databricks Asset Bundles (DABs) provide Infrastructure-as-Code for deploying
# MAGIC Databricks resources across environments.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Bundle Structure
# MAGIC
# MAGIC A typical bundle structure for this workshop:
# MAGIC
# MAGIC ```
# MAGIC illumia-workshop/
# MAGIC ├── databricks.yml           # Main bundle configuration
# MAGIC ├── resources/
# MAGIC │   ├── pipeline.yml         # Declarative pipeline definition
# MAGIC │   ├── job.yml              # Job definitions
# MAGIC │   └── dashboard.yml        # Dashboard configuration
# MAGIC ├── src/
# MAGIC │   ├── notebooks/
# MAGIC │   │   ├── 01_generate_data.py
# MAGIC │   │   └── 02_declarative_pipeline.py
# MAGIC │   └── python/
# MAGIC │       └── illumia/
# MAGIC │           └── transforms.py
# MAGIC └── tests/
# MAGIC     └── test_transforms.py
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Main Bundle Configuration
# MAGIC
# MAGIC The `databricks.yml` file defines the bundle and its targets:

# COMMAND ----------

# Example databricks.yml content
databricks_yml = """
bundle:
  name: illumia-workshop

variables:
  catalog:
    description: Unity Catalog for tables
    default: illumia_demo_catalog
  schema:
    description: Schema for tables
    default: workshop_data

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: ${DEV_WORKSPACE_HOST}
    variables:
      catalog: illumia_dev_catalog
      schema: dev_${bundle.target}_${var.user}

  staging:
    mode: development
    workspace:
      host: ${STAGING_WORKSPACE_HOST}
    variables:
      catalog: illumia_staging_catalog
      schema: staging_data

  prod:
    mode: production
    workspace:
      host: ${PROD_WORKSPACE_HOST}
    variables:
      catalog: illumia_prod_catalog
      schema: production_data
    permissions:
      - level: CAN_VIEW
        group_name: data-consumers
      - level: CAN_MANAGE
        group_name: data-engineers
"""

print("Example databricks.yml:")
print("=" * 60)
print(databricks_yml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Resource Definitions
# MAGIC
# MAGIC Define pipelines, jobs, and other resources in separate YAML files:

# COMMAND ----------

# Example pipeline.yml
pipeline_yml = """
resources:
  pipelines:
    illumia_pipeline:
      name: "${bundle.target}-illumia-pipeline"
      target: ${var.schema}
      catalog: ${var.catalog}
      libraries:
        - notebook:
            path: ../src/notebooks/02_declarative_pipeline.py
      configuration:
        pipeline.catalog: ${var.catalog}
        pipeline.schema: ${var.schema}
      clusters:
        - label: default
          autoscale:
            min_workers: 1
            max_workers: 4
            mode: ENHANCED
"""

print("Example resources/pipeline.yml:")
print("=" * 60)
print(pipeline_yml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 GitHub Actions Workflow
# MAGIC
# MAGIC Automate deployments with GitHub Actions:

# COMMAND ----------

# Example GitHub Actions workflow
github_workflow = """
name: Deploy Illumia Workshop

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main

      - name: Validate Bundle
        run: databricks bundle validate
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

  deploy-staging:
    needs: validate
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - uses: actions/checkout@v4

      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main

      - name: Deploy to Staging
        run: databricks bundle deploy --target staging
        env:
          DATABRICKS_HOST: ${{ secrets.STAGING_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.STAGING_TOKEN }}

      - name: Run Integration Tests
        run: databricks bundle run integration-tests --target staging
        env:
          DATABRICKS_HOST: ${{ secrets.STAGING_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.STAGING_TOKEN }}

  deploy-prod:
    needs: deploy-staging
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4

      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main

      - name: Deploy to Production
        run: databricks bundle deploy --target prod
        env:
          DATABRICKS_HOST: ${{ secrets.PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_TOKEN }}
"""

print("Example .github/workflows/deploy.yml:")
print("=" * 60)
print(github_workflow)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 Testing Strategies
# MAGIC
# MAGIC ### Unit Tests
# MAGIC Test transformation logic locally with pytest:
# MAGIC
# MAGIC ```python
# MAGIC # tests/test_transforms.py
# MAGIC import pytest
# MAGIC from illumia.transforms import calculate_engagement_score
# MAGIC
# MAGIC def test_engagement_score_high():
# MAGIC     score = calculate_engagement_score(
# MAGIC         total_transactions=100,
# MAGIC         unique_locations=10,
# MAGIC         total_spend=5000
# MAGIC     )
# MAGIC     assert score >= 80, "High activity should yield high score"
# MAGIC
# MAGIC def test_engagement_score_low():
# MAGIC     score = calculate_engagement_score(
# MAGIC         total_transactions=5,
# MAGIC         unique_locations=1,
# MAGIC         total_spend=50
# MAGIC     )
# MAGIC     assert score < 30, "Low activity should yield low score"
# MAGIC ```
# MAGIC
# MAGIC ### Integration Tests
# MAGIC Test against temporary schemas:
# MAGIC
# MAGIC ```python
# MAGIC # tests/integration/test_pipeline.py
# MAGIC def test_pipeline_creates_gold_tables(spark, test_schema):
# MAGIC     # Run pipeline against test schema
# MAGIC     # Verify gold tables exist with expected columns
# MAGIC     tables = spark.catalog.listTables(test_schema)
# MAGIC     assert "gold_cardholder_360" in [t.name for t in tables]
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 Deployment Commands
# MAGIC
# MAGIC Common bundle commands:
# MAGIC
# MAGIC ```bash
# MAGIC # Validate configuration
# MAGIC databricks bundle validate
# MAGIC
# MAGIC # Deploy to development (default target)
# MAGIC databricks bundle deploy
# MAGIC
# MAGIC # Deploy to specific target
# MAGIC databricks bundle deploy --target staging
# MAGIC databricks bundle deploy --target prod
# MAGIC
# MAGIC # Run a specific resource
# MAGIC databricks bundle run illumia_pipeline --target staging
# MAGIC
# MAGIC # Destroy resources (careful!)
# MAGIC databricks bundle destroy --target dev
# MAGIC
# MAGIC # View deployment status
# MAGIC databricks bundle summary --target prod
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.7 Rollback Procedures
# MAGIC
# MAGIC If a production deployment fails:
# MAGIC
# MAGIC 1. **Quick rollback**: Redeploy previous commit
# MAGIC    ```bash
# MAGIC    git checkout <previous-commit>
# MAGIC    databricks bundle deploy --target prod
# MAGIC    ```
# MAGIC
# MAGIC 2. **Data rollback**: Use Delta Time Travel
# MAGIC    ```sql
# MAGIC    -- Restore table to previous version
# MAGIC    RESTORE TABLE catalog.schema.gold_table TO VERSION AS OF 42
# MAGIC    ```
# MAGIC
# MAGIC 3. **Pipeline rollback**: Use update history
# MAGIC    ```python
# MAGIC    # Get pipeline history
# MAGIC    updates = w.pipelines.list_updates(pipeline_id=PIPELINE_ID)
# MAGIC    # Identify last successful update
# MAGIC    # Trigger refresh from known good state
# MAGIC    ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Workshop Part 2 Complete!
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC | Section | Key Concepts |
# MAGIC |---------|-------------|
# MAGIC | **Advanced Data Engineering** | Liquid Clustering, Change Data Feed, Time Travel, OPTIMIZE/VACUUM |
# MAGIC | **Pipeline Architecture** | SDP for declarative ETL, Notebooks for ML/imperative logic |
# MAGIC | **Security** | Row-Level Filtering, Column Masking, Encryption with Secrets, Tags, Audit |
# MAGIC | **CI/CD** | Asset Bundles, GitHub Actions, Testing, Rollback |
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Apply to your data**: Use these patterns on your production datasets
# MAGIC 2. **Set up CI/CD**: Create a bundle for your pipelines
# MAGIC 3. **Implement security**: Add RLF/CLM for sensitive data
# MAGIC 4. **Enable auditing**: Configure system table access for compliance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Remove the demonstration objects created in this notebook.

# COMMAND ----------

# UNCOMMENT TO CLEAN UP PART 2 RESOURCES
#
# # Remove row filter
# spark.sql(f"""
#     ALTER TABLE {USER_CATALOG}.{USER_SCHEMA}.gold_dining_operations
#     DROP ROW FILTER
# """)
#
# # Remove column mask
# spark.sql(f"""
#     ALTER TABLE {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360
#     ALTER COLUMN cardholder_name DROP MASK
# """)
#
# # Drop demonstration tables/views
# spark.sql(f"DROP TABLE IF EXISTS {USER_CATALOG}.{USER_SCHEMA}.gold_cardholder_360_clustered")
# spark.sql(f"DROP TABLE IF EXISTS {USER_CATALOG}.{USER_SCHEMA}.cardholder_sensitive_data")
# spark.sql(f"DROP VIEW IF EXISTS {USER_CATALOG}.{USER_SCHEMA}.v_cardholder_secure")
#
# # Drop functions
# spark.sql(f"DROP FUNCTION IF EXISTS {USER_CATALOG}.{USER_SCHEMA}.dining_location_filter")
# spark.sql(f"DROP FUNCTION IF EXISTS {USER_CATALOG}.{USER_SCHEMA}.mask_cardholder_name")
# spark.sql(f"DROP FUNCTION IF EXISTS {USER_CATALOG}.{USER_SCHEMA}.decrypt_student_id")
#
# print("Cleanup complete!")
