# Project 1: Real-Time Rental Listing Analytics Pipeline

## Overview

Build an end-to-end data engineering pipeline that ingests real-time rent listing events via Kafka, processes them with PySpark, and orchestrates the entire workflow using Apache Airflow. This project ties together all concepts from **Weeks 1–4** of the Data Engineering curriculum.

---

## Business Scenario

A property platform company wants to:

1. **Stream** listing events (new listings and rentals) in real time.
2. **Process** the raw events to compute hourly listings, most-rented apartments, and regional revenue breakdowns.
3. **Persist** both raw and transformed data to storage (local filesystem or S3).
4. **Orchestrate** the batch and streaming jobs on a daily schedule with retry and alerting.

---

## Architecture

```
┌──────────────┐       ┌─────────────┐       ┌─────────────────────┐
│ Listing Event│       │             │       │  PySpark Streaming   │
│  Simulator   │──────▶│   Kafka     │──────▶│  Consumer / ETL      │
│  (Producer)  │       │  (Topic:    │       │  (Spark Structured   │
│              │       │rental_listings)│    │   Streaming)          │
└──────────────┘       └─────────────┘       └──────────┬────────────┘
                                                        │
                                                        ▼
                                              ┌─────────────────────┐
                                              │  Raw Data Layer     │
                                              │  (Parquet / JSON)   │
                                              └──────────┬──────────┘
                                                         │
                                                         ▼
                                              ┌─────────────────────┐
                                              │  PySpark Batch ETL  │
                                              │  (Aggregations,     │
                                              │   Joins, Filters)   │
                                              └──────────┬──────────┘
                                                         │
                                                         ▼
                                              ┌─────────────────────┐
                                              │  Transformed Data   │
                                              │  (Parquet / CSV)    │
                                              └──────────┬──────────┘
                                                         │
                                                         ▼
                                              ┌─────────────────────┐
                                              │  Airflow DAG        │
                                              │  (Orchestration)    │
                                              └─────────────────────┘
```

---

## Tech Stack

| Technology     | Purpose                                      | Curriculum Week |
|----------------|----------------------------------------------|:---------------:|
| PySpark (RDDs) | Low-level data processing & custom transforms| Week 1          |
| PySpark (SQL)  | DataFrame operations, aggregations, joins    | Week 2          |
| Apache Kafka   | Real-time event ingestion (producer/consumer)| Week 3          |
| Spark Streaming| Consuming Kafka topics in near real-time     | Week 3          |
| Apache Airflow | DAG-based job orchestration & scheduling     | Week 4          |

---

## Detailed Requirements

### Module 1 — Kafka Producer (Week 3)

**Goal:** Simulate a stream of listing events.

- Create a Kafka topic named `rental_listings`.
- Write a Python Kafka producer (`producer.py`) that generates JSON listing events:
  ```json
  {
    "property_id": "Th50000000",
    "customer_id": "CUST-301",
    "building_type": "Apartment",
    "year_built": 1990,
    "posted_on": "2022-04-20T12:32:00Z",
    "rented_on": "2022-05-05T14:32:00Z",
    "bedrooms": 2,
    "size": 993,
    "rent": 4145,
    "duration": 12,
    "rental_status": "rented",
    "city": "Brooklyn"
  }
  ```
- Use `rental_status` values: `open`, `rented`.
- If rental_status is open, set rented_on to null
- Produce at least **500 events** with randomized data using the `Faker` library.

---

### Module 2 — Spark Streaming Consumer (Week 3)

**Goal:** Consume and persist the raw Kafka stream.

- Write a PySpark Structured Streaming job (`stream_consumer.py`).
- Read from the `rental_listings` Kafka topic.
- Deserialize JSON messages into a Spark DataFrame.
- Write the raw data to a **Parquet** sink partitioned by `date` (derived from `posted_on`).
- Implement a 1-minute micro-batch trigger.

---

### Module 3 — Batch ETL with PySpark (Weeks 1 & 2)

**Goal:** Transform raw data into analytics-ready datasets.

#### 3A — RDD-Based Processing (Week 1)

- Load the raw Parquet data as an RDD.
- Use RDD transformations (`map`, `filter`, `reduceByKey`) to:
  - Filter out 'open' listings (keeping rented listings).
  - Compute total revenue per 'property_id' using key-value pair RDDs.
- Save the result as a text file.

#### 3B — DataFrame / Spark SQL Processing (Week 2)

- Load the raw Parquet data into a Spark DataFrame.
- Perform the following transformations:
  1. **Hourly Rentals Summary** — Group by hour, compute `total_rentals`, `total_revenue`, `avg_rental_value`.
  2. **Rentals by Bedroom Count** — Rank bedroom counts by total number of rentals using Spark SQL window functions.
  3. **State Revenue** — Join listings with a static states.csv reference dataset (mapping city → state), then aggregate total rental revenue by state.
  4. **Rental Status Breakdown** — Pivot on `rental_status` to get counts per category.
- Write each output to Parquet, partitioned and bucketed where appropriate.
- Use **caching** on the base DataFrame to speed up multiple downstream transformations.

---

### Module 4 — Airflow Orchestration (Week 4)

**Goal:** Schedule and manage the full pipeline.

- Create an Airflow DAG named `rental_pipeline` in a file called `rental_dag.py`.
- Define the following tasks with proper dependencies:

  ```
  start >> check_kafka_topic >> run_streaming_job >> wait_for_raw_data
        >> run_rdd_etl >> run_df_etl >> validate_output >> end
  ```

- **Task details:**

  | Task                 | Operator Type       | Description                                      |
  |----------------------|---------------------|--------------------------------------------------|
  | `start`              | DummyOperator       | Pipeline entry point                             |
  | `check_kafka_topic`  | PythonOperator      | Verify the Kafka topic exists and has messages   |
  | `run_streaming_job`  | BashOperator        | Submit the Spark Streaming job via `spark-submit` |
  | `wait_for_raw_data`  | FileSensor          | Wait until raw Parquet files appear              |
  | `run_rdd_etl`        | BashOperator        | Submit the RDD batch job                         |
  | `run_df_etl`         | BashOperator        | Submit the DataFrame batch job                   |
  | `validate_output`    | PythonOperator      | Check row counts & schema of output files        |
  | `end`                | DummyOperator       | Pipeline exit point                              |

- Configure:
  - `schedule_interval`: `@daily`
  - `retries`: 2, `retry_delay`: 5 minutes
  - `email_on_failure`: `true`
  - Use **Connections** for Kafka broker and Spark cluster settings.
  - Create at least one **parameterized DAG** that accepts `execution_date` as a parameter.

---

## Deliverables

| #  | Deliverable                        | Format              |
|----|------------------------------------|----------------------|
| 1  | `producer.py`                      | Python script        |
| 2  | `stream_consumer.py`               | PySpark script       |
| 3  | `batch_rdd_etl.py`                 | PySpark script       |
| 4  | `batch_df_etl.py`                  | PySpark script       |
| 5  | `rental_dag.py`                 | Airflow DAG          |
| 6  | `states.csv`                      | Reference data       |
| 7  | `README.md`                        | Setup & run guide    |
| 8  | Sample output screenshots          | PNG / Markdown       |

---

## Folder Structure

```
project1/
├── README.md
├── data/
│   ├── states.csv
│   ├── raw/                  # Raw Parquet output from streaming
│   └── transformed/          # Aggregated Parquet output from batch ETL
├── kafka/
│   └── producer.py
├── spark/
│   ├── stream_consumer.py
│   ├── batch_rdd_etl.py
│   └── batch_df_etl.py
├── airflow/
│   └── dags/
│       └── rental_dag.py
└── config/
    └── spark-defaults.conf
```

---

## Evaluation Criteria

| Area                     | Weight | What We Look For                                              |
|--------------------------|:------:|---------------------------------------------------------------|
| Kafka Integration        | 20%    | Proper topic setup, message schema, producer reliability      |
| Spark Streaming          | 15%    | Correct consumption, deserialization, partitioned Parquet sink |
| RDD Processing           | 15%    | Use of transformations, key-value RDDs, accumulators          |
| DataFrame / Spark SQL    | 20%    | Aggregations, joins, window functions, caching, bucketing     |
| Airflow DAG              | 20%    | Task dependencies, operator usage, parameterization, retries  |
| Code Quality & Docs      | 10%    | Clean code, README, inline comments, reproducibility          |

---

## Stretch Goals (Optional)

- Deploy the Spark jobs on an **AWS EMR** cluster (Week 1 - Friday).
- Use **Spark accumulators** to track bad/malformed records during RDD processing.
- Add a second Kafka topic 'listing_updates` for status changes and join both streams.
- Implement **dynamic DAGs** in Airflow that auto-generate tasks based on a config file.
- Add data quality checks using assertions in the `validate_output` task.