-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Illumia Dashboard Queries
-- MAGIC
-- MAGIC Pre-built SQL queries for the 4-widget Lakeview dashboard:
-- MAGIC 1. **Revenue by Location** - gold_location_analytics
-- MAGIC 2. **Food Waste by Dining Hall** - gold_dining_operations
-- MAGIC 3. **Cardholder Engagement Distribution** - gold_cardholder_360
-- MAGIC 4. **Spending by Housing Area** - gold_behavior_patterns
-- MAGIC
-- MAGIC **AWS Equivalent**: QuickSight dataset queries
-- MAGIC
-- MAGIC Replace `{USER_ID}` with your participant ID before using.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Widget 1: Revenue by Location
-- MAGIC
-- MAGIC Shows total revenue and transaction count by merchant location.
-- MAGIC Visualize as: **Bar Chart** (horizontal)

-- COMMAND ----------

-- Query 1: Revenue by Location (Bar Chart)
-- Dataset: gold_location_analytics
SELECT
    merchant_name,
    merchant_category,
    SUM(total_revenue) AS total_revenue,
    SUM(transaction_count) AS total_transactions,
    ROUND(AVG(avg_transaction), 2) AS avg_transaction_value,
    SUM(unique_customers) AS total_unique_customers
FROM ${catalog}.${schema}.${user_id}_gold_location_analytics
GROUP BY merchant_name, merchant_category
ORDER BY total_revenue DESC
LIMIT 15

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Widget 2: Food Waste by Dining Hall
-- MAGIC
-- MAGIC Shows waste percentage by dining location and meal period.
-- MAGIC Visualize as: **Bar Chart** (grouped by meal period)

-- COMMAND ----------

-- Query 2: Food Waste by Dining Hall (Bar Chart)
-- Dataset: gold_dining_operations
SELECT
    location_name,
    meal_period,
    ROUND(AVG(waste_percentage), 1) AS avg_waste_pct,
    SUM(total_wasted) AS total_portions_wasted,
    SUM(total_served) AS total_portions_served,
    ROUND(SUM(total_food_cost), 2) AS total_food_cost,
    ROUND(AVG(efficiency_rate), 1) AS avg_efficiency_rate
FROM ${catalog}.${schema}.${user_id}_gold_dining_operations
GROUP BY location_name, meal_period
ORDER BY location_name,
    CASE meal_period
        WHEN 'breakfast' THEN 1
        WHEN 'lunch' THEN 2
        WHEN 'dinner' THEN 3
        WHEN 'late_night' THEN 4
    END

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Widget 3: Cardholder Engagement Distribution
-- MAGIC
-- MAGIC Shows distribution of cardholders across engagement tiers.
-- MAGIC Visualize as: **Pie Chart** or **Counter** cards

-- COMMAND ----------

-- Query 3: Cardholder Engagement Distribution (Pie Chart)
-- Dataset: gold_cardholder_360
SELECT
    engagement_tier,
    patron_type_clean AS patron_type,
    COUNT(*) AS cardholder_count,
    ROUND(AVG(engagement_score), 1) AS avg_engagement_score,
    ROUND(AVG(total_spend), 2) AS avg_total_spend,
    ROUND(AVG(total_transactions), 1) AS avg_transactions,
    ROUND(AVG(unique_buildings), 1) AS avg_buildings_visited
FROM ${catalog}.${schema}.${user_id}_gold_cardholder_360
WHERE is_active = true
GROUP BY engagement_tier, patron_type_clean
ORDER BY
    CASE engagement_tier
        WHEN 'high' THEN 1
        WHEN 'medium' THEN 2
        WHEN 'low' THEN 3
    END,
    cardholder_count DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Widget 4: Spending by Housing Area
-- MAGIC
-- MAGIC The cross-domain insight: spending patterns vary by where students live.
-- MAGIC Visualize as: **Bar Chart** (with avg spend per customer)

-- COMMAND ----------

-- Query 4: Spending by Housing Area (Bar Chart)
-- Dataset: gold_behavior_patterns
SELECT
    housing_area,
    patron_type_clean AS patron_type,
    transaction_count,
    ROUND(total_spend, 2) AS total_spend,
    unique_customers,
    ROUND(avg_spend_per_customer, 2) AS avg_spend_per_customer,
    ROUND(transactions_per_customer, 1) AS transactions_per_customer,
    ROUND(weekend_ratio * 100, 1) AS weekend_pct
FROM ${catalog}.${schema}.${user_id}_gold_behavior_patterns
WHERE housing_area IS NOT NULL
ORDER BY avg_spend_per_customer DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bonus: Summary Statistics

-- COMMAND ----------

-- Bonus: Summary Stats for Counter Widgets
-- These can be used for KPI counter widgets at the top of the dashboard

-- Total Revenue
SELECT
    'Total Revenue' AS metric,
    CONCAT('$', FORMAT_NUMBER(SUM(total_revenue), 0)) AS value
FROM ${catalog}.${schema}.${user_id}_gold_location_analytics

UNION ALL

-- Total Transactions
SELECT
    'Total Transactions' AS metric,
    FORMAT_NUMBER(SUM(transaction_count), 0) AS value
FROM ${catalog}.${schema}.${user_id}_gold_location_analytics

UNION ALL

-- Active Cardholders
SELECT
    'Active Cardholders' AS metric,
    FORMAT_NUMBER(COUNT(*), 0) AS value
FROM ${catalog}.${schema}.${user_id}_gold_cardholder_360
WHERE is_active = true

UNION ALL

-- Avg Waste Percentage
SELECT
    'Avg Food Waste' AS metric,
    CONCAT(ROUND(AVG(waste_percentage), 1), '%') AS value
FROM ${catalog}.${schema}.${user_id}_gold_dining_operations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Parameter Reference
-- MAGIC
-- MAGIC When using these queries in the Lakeview API or dashboard creation, replace:
-- MAGIC - `${catalog}` → `illumia_demo_catalog`
-- MAGIC - `${schema}` → `workshop_data` or your user schema
-- MAGIC - `${user_id}` → Your participant ID (e.g., `austin_rosenberg`)
