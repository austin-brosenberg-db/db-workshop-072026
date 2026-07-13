# Databricks notebook source
# MAGIC %md
# MAGIC # Illumia Campus Platform - Data Generation
# MAGIC
# MAGIC This notebook generates realistic synthetic data for the Illumia campus operations platform:
# MAGIC - **Cardholders**: Student/staff identity records (CS Gold system)
# MAGIC - **Transactions**: POS purchases and meal swipes (GET platform)
# MAGIC - **Access Events**: Building entry/exit events (CS Access system)
# MAGIC - **Food Service**: Dining hall production/waste data (NetMenu system)
# MAGIC
# MAGIC **AWS Equivalent**: Running multiple Glue ETL jobs to populate S3 with test data.

# COMMAND ----------

# MAGIC %pip install dbldatagen faker --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql import functions as F
from pyspark.sql.types import *
from faker import Faker
import random
from datetime import datetime, timedelta
import json

# Keep Python's built-in round
_round = round

# Initialize Faker for realistic names
fake = Faker()
Faker.seed(42)
random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set up the catalog, schema, and volume paths for data storage.

# COMMAND ----------

# Widget parameters - set by job or manually
dbutils.widgets.text("catalog", "illumia_demo_catalog")
dbutils.widgets.text("schema", "workshop_data")

# COMMAND ----------

# Configuration from widgets
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}"

# Data generation parameters
NUM_INSTITUTIONS = 5
NUM_CARDHOLDERS = 5000
NUM_TRANSACTIONS = 100000  # Reduced for faster generation
NUM_ACCESS_EVENTS = 200000  # Reduced for faster generation
NUM_FOOD_SERVICE_RECORDS = 10000  # Reduced for faster generation
DATE_RANGE_DAYS = 90

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Volume Base: {VOLUME_BASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Schema and Volumes

# COMMAND ----------

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Create volumes for each data type
for volume_name in ["cardholders", "transactions", "access_events", "food_service", "checkpoints"]:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{volume_name}")
    print(f"Created volume: {CATALOG}.{SCHEMA}.{volume_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Data
# MAGIC
# MAGIC Define institutions, locations, merchants, and buildings for realistic data generation.

# COMMAND ----------

# Institution reference data
INSTITUTIONS = [
    {"id": "INST-UMICH", "name": "University of Michigan", "state": "MI"},
    {"id": "INST-OSU", "name": "Ohio State University", "state": "OH"},
    {"id": "INST-PSU", "name": "Penn State University", "state": "PA"},
    {"id": "INST-MSU", "name": "Michigan State University", "state": "MI"},
    {"id": "INST-IU", "name": "Indiana University", "state": "IN"},
]

# Housing areas per campus (for behavioral patterns)
HOUSING_AREAS = ["north_campus", "central_campus", "south_campus", "off_campus", "greek_row"]

# Patron types with distribution weights
PATRON_TYPES = [
    ("student", 0.75),
    ("staff", 0.15),
    ("faculty", 0.08),
    ("visitor", 0.02)
]

# Meal plan types
MEAL_PLANS = ["unlimited", "block_200", "block_150", "block_100", "commuter", None]

# Merchant categories and locations
MERCHANTS = [
    {"id": "MERCH-001", "name": "Main Dining Hall", "category": "dining", "area": "central_campus"},
    {"id": "MERCH-002", "name": "North Campus Cafe", "category": "dining", "area": "north_campus"},
    {"id": "MERCH-003", "name": "South Side Grill", "category": "dining", "area": "south_campus"},
    {"id": "MERCH-004", "name": "Campus Coffee", "category": "dining", "area": "central_campus"},
    {"id": "MERCH-005", "name": "Late Night Bites", "category": "dining", "area": "central_campus"},
    {"id": "MERCH-006", "name": "Student Bookstore", "category": "retail", "area": "central_campus"},
    {"id": "MERCH-007", "name": "Campus Convenience", "category": "retail", "area": "north_campus"},
    {"id": "MERCH-008", "name": "Print & Copy Center", "category": "services", "area": "central_campus"},
    {"id": "MERCH-009", "name": "Laundry Services", "category": "services", "area": "north_campus"},
    {"id": "MERCH-010", "name": "Parking Services", "category": "parking", "area": "central_campus"},
    {"id": "MERCH-011", "name": "Vending - Library", "category": "vending", "area": "central_campus"},
    {"id": "MERCH-012", "name": "Vending - Gym", "category": "vending", "area": "south_campus"},
    {"id": "MERCH-013", "name": "Downtown Pizza", "category": "off_campus", "area": "off_campus"},
    {"id": "MERCH-014", "name": "Local BBQ Joint", "category": "off_campus", "area": "off_campus"},
    {"id": "MERCH-015", "name": "Asian Fusion", "category": "off_campus", "area": "off_campus"},
]

# Buildings for access control
BUILDINGS = [
    {"id": "BLDG-001", "name": "North Residence Hall", "type": "residence_hall", "zone": "north_campus"},
    {"id": "BLDG-002", "name": "Central Residence Hall", "type": "residence_hall", "zone": "central_campus"},
    {"id": "BLDG-003", "name": "South Residence Hall", "type": "residence_hall", "zone": "south_campus"},
    {"id": "BLDG-004", "name": "Engineering Building", "type": "academic", "zone": "north_campus"},
    {"id": "BLDG-005", "name": "Liberal Arts Hall", "type": "academic", "zone": "central_campus"},
    {"id": "BLDG-006", "name": "Science Complex", "type": "academic", "zone": "south_campus"},
    {"id": "BLDG-007", "name": "Main Library", "type": "library", "zone": "central_campus"},
    {"id": "BLDG-008", "name": "Recreation Center", "type": "recreation", "zone": "south_campus"},
    {"id": "BLDG-009", "name": "Student Union", "type": "student_services", "zone": "central_campus"},
    {"id": "BLDG-010", "name": "Administration Building", "type": "admin", "zone": "central_campus"},
]

# Food service menu items
MENU_ITEMS = [
    {"name": "Pasta Primavera", "category": "entree", "calories": 420, "cost": 1.85, "diet_tags": ["vegetarian"]},
    {"name": "Grilled Chicken", "category": "entree", "calories": 380, "cost": 2.25, "diet_tags": []},
    {"name": "BBQ Pulled Pork", "category": "entree", "calories": 520, "cost": 2.50, "diet_tags": []},
    {"name": "Veggie Stir Fry", "category": "entree", "calories": 350, "cost": 1.75, "diet_tags": ["vegetarian", "vegan"]},
    {"name": "Fish Tacos", "category": "entree", "calories": 450, "cost": 2.75, "diet_tags": []},
    {"name": "Caesar Salad", "category": "salad", "calories": 280, "cost": 1.50, "diet_tags": []},
    {"name": "Garden Salad", "category": "salad", "calories": 150, "cost": 1.25, "diet_tags": ["vegetarian", "vegan"]},
    {"name": "Tomato Soup", "category": "soup", "calories": 180, "cost": 0.95, "diet_tags": ["vegetarian"]},
    {"name": "Chicken Noodle Soup", "category": "soup", "calories": 220, "cost": 1.10, "diet_tags": []},
    {"name": "French Fries", "category": "side", "calories": 320, "cost": 0.85, "diet_tags": ["vegetarian", "vegan"]},
    {"name": "Steamed Vegetables", "category": "side", "calories": 80, "cost": 0.75, "diet_tags": ["vegetarian", "vegan"]},
    {"name": "Brownie", "category": "dessert", "calories": 380, "cost": 0.65, "diet_tags": ["vegetarian"]},
    {"name": "Fresh Fruit Cup", "category": "dessert", "calories": 120, "cost": 1.00, "diet_tags": ["vegetarian", "vegan"]},
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate Cardholders Data

# COMMAND ----------

def generate_cardholders(num_records: int, institutions: list) -> list:
    """Generate cardholder records."""
    records = []

    for i in range(num_records):
        institution = random.choice(institutions)
        patron_type = random.choices(
            [p[0] for p in PATRON_TYPES],
            weights=[p[1] for p in PATRON_TYPES]
        )[0]

        # Generate realistic data based on patron type
        if patron_type == "student":
            class_year = str(random.randint(2024, 2028))
            department = random.choice(["Computer Science", "Engineering", "Business", "Biology", "Psychology", "English", "Mathematics", "Chemistry", "Physics", "Art"])
            meal_plan = random.choice(MEAL_PLANS[:5])  # Students have meal plans
            housing_area = random.choice(HOUSING_AREAS)
        else:
            class_year = None
            department = random.choice(["Administration", "Facilities", "IT Services", "Academic Affairs", "Student Services", "Athletics", "Research"])
            meal_plan = random.choice([None, "commuter"])
            housing_area = "off_campus"

        cardholder = {
            "cardholder_id": f"CH-{institution['id'][-5:]}-{str(i).zfill(6)}",
            "institution_id": institution["id"],
            "patron_type": patron_type,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": f"{fake.user_name()}@{institution['name'].lower().replace(' ', '')}.edu",
            "status": random.choices(["active", "inactive", "suspended", "graduated"], weights=[0.85, 0.05, 0.02, 0.08])[0],
            "department": department,
            "class_year": class_year,
            "housing_area": housing_area,
            "meal_plan_type": meal_plan,
            "stored_value_balance": _round(random.uniform(0, 500), 2),
            "created_at": (datetime.now() - timedelta(days=random.randint(30, 365))).isoformat() + "Z",
            "updated_at": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat() + "Z"
        }
        records.append(cardholder)

    return records

print("Generating cardholders...")
cardholders_data = generate_cardholders(NUM_CARDHOLDERS, INSTITUTIONS)
print(f"Generated {len(cardholders_data)} cardholder records")

# COMMAND ----------

# Convert to DataFrame and write as JSON
cardholders_df = spark.createDataFrame(cardholders_data)
cardholders_df.write.mode("overwrite").json(f"{VOLUME_BASE}/cardholders/")
print(f"Wrote cardholders to {VOLUME_BASE}/cardholders/")
display(cardholders_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate Transactions Data

# COMMAND ----------

def generate_transactions(num_records: int, cardholders: list, merchants: list, date_range_days: int) -> list:
    """Generate transaction records with realistic patterns."""
    records = []
    active_cardholders = [c for c in cardholders if c["status"] == "active"]

    for i in range(num_records):
        cardholder = random.choice(active_cardholders)
        merchant = random.choice(merchants)

        # Generate timestamp with realistic patterns (more during lunch/dinner)
        days_ago = random.randint(0, date_range_days)
        base_date = datetime.now() - timedelta(days=days_ago)

        # Weight towards meal times
        hour_weights = [0.5] * 6 + [1, 2, 3] + [1] * 2 + [4, 5, 4] + [1] * 3 + [3, 4, 3] + [2, 1, 0.5, 0.5]
        hour = random.choices(range(24), weights=hour_weights)[0]
        minute = random.randint(0, 59)
        timestamp = base_date.replace(hour=hour, minute=minute, second=random.randint(0, 59))

        # Transaction amount based on merchant category
        if merchant["category"] == "dining":
            amount = _round(random.uniform(5.00, 18.00), 2)
        elif merchant["category"] == "retail":
            amount = _round(random.uniform(2.00, 75.00), 2)
        elif merchant["category"] == "vending":
            amount = _round(random.uniform(1.00, 5.00), 2)
        elif merchant["category"] == "off_campus":
            amount = _round(random.uniform(8.00, 35.00), 2)
        else:
            amount = _round(random.uniform(1.00, 25.00), 2)

        # Payment method varies by patron type
        if cardholder["meal_plan_type"] and merchant["category"] == "dining":
            payment_method = random.choices(["meal_plan", "stored_value"], weights=[0.7, 0.3])[0]
        else:
            payment_method = random.choices(["stored_value", "credit_card"], weights=[0.6, 0.4])[0]

        transaction = {
            "transaction_id": f"TXN-{timestamp.strftime('%Y%m%d')}-{str(i).zfill(10)}",
            "cardholder_id": cardholder["cardholder_id"],
            "institution_id": cardholder["institution_id"],
            "transaction_type": random.choices(["purchase", "refund"], weights=[0.98, 0.02])[0],
            "payment_method": payment_method,
            "merchant_id": merchant["id"],
            "merchant_name": merchant["name"],
            "merchant_category": merchant["category"],
            "location_area": merchant["area"],
            "amount": amount,
            "timestamp": timestamp.isoformat() + "Z"
        }
        records.append(transaction)

    return records

print("Generating transactions...")
transactions_data = generate_transactions(NUM_TRANSACTIONS, cardholders_data, MERCHANTS, DATE_RANGE_DAYS)
print(f"Generated {len(transactions_data)} transaction records")

# COMMAND ----------

# Write transactions in batches for efficiency
transactions_df = spark.createDataFrame(transactions_data)
transactions_df.write.mode("overwrite").json(f"{VOLUME_BASE}/transactions/")
print(f"Wrote transactions to {VOLUME_BASE}/transactions/")
display(transactions_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate Access Events Data

# COMMAND ----------

def generate_access_events(num_records: int, cardholders: list, buildings: list, date_range_days: int) -> list:
    """Generate building access event records."""
    records = []
    active_cardholders = [c for c in cardholders if c["status"] == "active"]

    for i in range(num_records):
        cardholder = random.choice(active_cardholders)
        building = random.choice(buildings)

        # Generate timestamp
        days_ago = random.randint(0, date_range_days)
        base_date = datetime.now() - timedelta(days=days_ago)

        # Access patterns: residence halls have 24/7 access, academic buildings peak during day
        if building["type"] == "residence_hall":
            hour = random.randint(0, 23)
        elif building["type"] in ["library", "recreation"]:
            hour = random.choices(range(24), weights=[0.1]*6 + [0.5, 1, 2, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 1, 0.5, 0.3, 0.2])[0]
        else:
            hour = random.choices(range(24), weights=[0.1]*7 + [2, 3, 4, 4, 3, 3, 4, 4, 3, 2, 1, 0.5, 0.3, 0.2, 0.1, 0.1, 0.1])[0]

        timestamp = base_date.replace(hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59))

        # Event type - mostly granted, some denials
        event_type = random.choices(
            ["access_granted", "access_denied"],
            weights=[0.97, 0.03]
        )[0]

        denial_reason = None
        if event_type == "access_denied":
            denial_reason = random.choice(["invalid_credential", "expired", "no_access_rights", "time_restriction"])

        event = {
            "event_id": f"ACC-{timestamp.strftime('%Y%m%d')}-{str(i).zfill(10)}",
            "cardholder_id": cardholder["cardholder_id"],
            "institution_id": cardholder["institution_id"],
            "event_type": event_type,
            "door_id": f"DOOR-{building['id']}-MAIN",
            "door_name": f"{building['name']} Main Entrance",
            "building_id": building["id"],
            "building_name": building["name"],
            "building_type": building["type"],
            "zone": building["zone"],
            "direction": random.choice(["entry", "exit"]),
            "denial_reason": denial_reason,
            "timestamp": timestamp.isoformat() + "Z"
        }
        records.append(event)

    return records

print("Generating access events...")
access_events_data = generate_access_events(NUM_ACCESS_EVENTS, cardholders_data, BUILDINGS, DATE_RANGE_DAYS)
print(f"Generated {len(access_events_data)} access event records")

# COMMAND ----------

# Write access events
access_events_df = spark.createDataFrame(access_events_data)
access_events_df.write.mode("overwrite").json(f"{VOLUME_BASE}/access_events/")
print(f"Wrote access events to {VOLUME_BASE}/access_events/")
display(access_events_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate Food Service Data

# COMMAND ----------

def generate_food_service(num_records: int, institutions: list, menu_items: list, date_range_days: int) -> list:
    """Generate food service production/waste records."""
    records = []

    dining_locations = [
        {"id": "LOC-DINING-001", "name": "Main Dining Hall"},
        {"id": "LOC-DINING-002", "name": "North Campus Dining"},
        {"id": "LOC-DINING-003", "name": "South Campus Dining"},
    ]

    meal_periods = ["breakfast", "lunch", "dinner", "late_night"]

    for i in range(num_records):
        institution = random.choice(institutions)
        location = random.choice(dining_locations)
        menu_item = random.choice(menu_items)

        # Generate service date
        days_ago = random.randint(0, date_range_days)
        service_date = (datetime.now() - timedelta(days=days_ago)).date()

        # Meal period with weights
        meal_period = random.choices(meal_periods, weights=[0.15, 0.35, 0.40, 0.10])[0]

        # Production planning - varies by meal period and item
        base_portions = random.randint(50, 300)
        if meal_period == "breakfast":
            base_portions = int(base_portions * 0.6)
        elif meal_period == "late_night":
            base_portions = int(base_portions * 0.4)

        planned_portions = base_portions
        # Actual portions served is usually less than planned
        portions_served = int(planned_portions * random.uniform(0.70, 0.95))
        # Waste is what's left over
        portions_wasted = max(0, int(planned_portions * random.uniform(0.03, 0.15)))

        record = {
            "record_id": f"FS-{service_date.strftime('%Y%m%d')}-{str(i).zfill(8)}",
            "institution_id": institution["id"],
            "location_id": location["id"],
            "location_name": location["name"],
            "meal_period": meal_period,
            "service_date": service_date.isoformat(),
            "menu_item_name": menu_item["name"],
            "category": menu_item["category"],
            "diet_tags": menu_item["diet_tags"],
            "planned_portions": planned_portions,
            "portions_served": portions_served,
            "portions_wasted": portions_wasted,
            "calories": menu_item["calories"],
            "food_cost_per_portion": menu_item["cost"],
            "timestamp": datetime.now().isoformat() + "Z"
        }
        records.append(record)

    return records

print("Generating food service records...")
food_service_data = generate_food_service(NUM_FOOD_SERVICE_RECORDS, INSTITUTIONS, MENU_ITEMS, DATE_RANGE_DAYS)
print(f"Generated {len(food_service_data)} food service records")

# COMMAND ----------

# Write food service data
food_service_df = spark.createDataFrame(food_service_data)
food_service_df.write.mode("overwrite").json(f"{VOLUME_BASE}/food_service/")
print(f"Wrote food service to {VOLUME_BASE}/food_service/")
display(food_service_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("DATA GENERATION COMPLETE")
print("=" * 60)
print(f"\nCatalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"\nData Volumes:")
print(f"  - Cardholders:    {VOLUME_BASE}/cardholders/")
print(f"  - Transactions:   {VOLUME_BASE}/transactions/")
print(f"  - Access Events:  {VOLUME_BASE}/access_events/")
print(f"  - Food Service:   {VOLUME_BASE}/food_service/")
print(f"\nRecord Counts:")
print(f"  - Cardholders:    {len(cardholders_data):,}")
print(f"  - Transactions:   {len(transactions_data):,}")
print(f"  - Access Events:  {len(access_events_data):,}")
print(f"  - Food Service:   {len(food_service_data):,}")
print(f"\nTotal Records: {len(cardholders_data) + len(transactions_data) + len(access_events_data) + len(food_service_data):,}")
