# Illumia Campus Analytics Workshop

A hands-on Databricks workshop building a complete analytics platform for campus operations.

## Overview

Build a production-ready data platform in ~2.5 hours:

- **Data Ingestion** - Autoloader from 4 source systems
- **Transformation** - Declarative Pipelines (Bronze → Silver → Gold)
- **Visualization** - Lakeview Dashboard
- **AI Assistant** - Embedded Genie Agent
- **Deployment** - Databricks App

## About Illumia

[Illumia](https://illumiatech.com/) (formerly CBORD + Transact) powers payments, access control, and food service at over 10,000 campuses, healthcare facilities, and senior living communities. This workshop uses realistic synthetic data from four Illumia systems:

| System | Data |
|--------|------|
| CS Gold | Cardholder identity records |
| GET Platform | POS transactions and meal swipes |
| CS Access | Building entry/exit events |
| NetMenu | Dining hall operations and waste |

## Repository Structure

```
db-workshop-072026/
├── README.md
├── app/
│   ├── app.yaml                 # App configuration (env vars)
│   ├── server/
│   │   ├── main.py              # FastAPI backend
│   │   ├── config.py            # Configuration helpers
│   │   └── genie_client.py      # Genie API client
│   └── frontend/
│       └── src/
│           ├── App.tsx          # Main app component
│           └── components/
│               ├── ChatInterface.tsx    # Genie chat UI
│               └── DashboardEmbed.tsx   # Dashboard embed
└── notebooks/
    ├── workshop_driver.py       # Main workshop notebook
    ├── 01_generate_data.py      # Data generation
    ├── 02_declarative_pipeline.py  # DP definition
    └── 03_dashboard_queries.sql # Dashboard SQL queries
```

## Prerequisites

- Databricks workspace with:
  - Unity Catalog enabled
  - Serverless compute available
  - Genie enabled
  - Apps enabled
- Workshop catalog `illumia_demo_catalog` with appropriate permissions

## Getting Started

1. Open `notebooks/workshop_driver.py` in your Databricks workspace
2. Attach to a cluster with the Databricks SDK installed
3. Run each cell in order, following the instructions

## App Configuration

The app uses environment variables for participant-specific configuration:

| Variable | Description |
|----------|-------------|
| `GENIE_SPACE_ID` | Your Genie Space ID from Step 8 |
| `DASHBOARD_ID` | Your Dashboard ID from Step 7 |

These are set in `app/app.yaml` during Step 9.

## Gold Tables

The Declarative Pipeline creates four gold tables for analytics:

| Table | Description |
|-------|-------------|
| `gold_cardholder_360` | Unified view combining transactions and access data |
| `gold_location_analytics` | Revenue and traffic by location and time |
| `gold_dining_operations` | Food waste and production efficiency |
| `gold_behavior_patterns` | Cross-domain behavioral insights |

## Cleanup

To remove all workshop resources, uncomment and run the cleanup cell at the end of `workshop_driver.py`, or manually delete:

- Apps → Delete `illumia-{user_id}`
- Jobs → Delete `{user_id}_illumia_data_generation`
- Pipelines → Delete `{user_id}_illumia_pipeline`
- Catalog → Drop schema `illumia_demo_catalog.{user_id}`
