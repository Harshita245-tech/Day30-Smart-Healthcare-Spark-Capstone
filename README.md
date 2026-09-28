# 🏥 Day 30 — Smart Healthcare Monitoring & Patient Risk Analytics Platform

## 📌 Project Overview

Day 30 is the final Apache Spark and Scala capstone project developed as part of the Nexturn Data Engineering training.

The project implements a **Smart Healthcare Monitoring & Patient Risk Analytics Platform** using Apache Spark.

The platform combines:

- Historical healthcare data processing
- Patient and hospital information
- Patient vital-sign analysis
- Medication information
- Patient risk scoring
- Spark SQL analytics
- Window functions
- Broadcast joins
- User Defined Functions (UDF)
- Accumulators
- Cache and persistence
- Partition tuning
- DAG and execution-plan analysis
- Real-time streaming vital monitoring
- Abnormal vital detection
- Streaming risk analysis
- Window-based alert processing

The project demonstrates how Apache Spark can be used for both **large-scale batch analytics and real-time healthcare monitoring**.

---

# 🎯 Project Objectives

The major objectives of the project are:

1. Process historical patient vital information.
2. Integrate patient, hospital and medication datasets.
3. Clean and validate healthcare records.
4. Identify abnormal vital readings.
5. Calculate patient risk scores.
6. Classify patients into LOW, MEDIUM and HIGH risk levels.
7. Apply Spark SQL for analytical reporting.
8. Demonstrate window functions for patient-level analysis.
9. Optimize joins using broadcast operations.
10. Demonstrate caching and persistence.
11. Demonstrate accumulators for invalid-record tracking.
12. Demonstrate repartitioning and partition tuning.
13. Analyze Spark DAG and physical execution plans.
14. Process live healthcare vital events using Spark Streaming.
15. Detect abnormal streaming vital readings.
16. Generate real-time patient risk alerts.
17. Demonstrate time-based streaming window operations.

---

# 🏗️ Overall Architecture

```text
                         SMART HEALTHCARE PLATFORM
                                  │
                 ┌────────────────┴────────────────┐
                 │                                 │
          BATCH PROCESSING                  REAL-TIME STREAMING
                 │                                 │
                 ▼                                 ▼
        Historical CSV Data                  Live Vital Events
                 │                                 │
                 ▼                                 ▼
          Data Cleaning                       Socket Source
                 │                                 │
                 ▼                                 ▼
        Patient/Vital Join                  5-Second Batches
                 │                                 │
                 ▼                                 ▼
        Hospital Broadcast                 Validation
                 │                                 │
                 ▼                                 ▼
        Medication Join                    Abnormal Detection
                 │                                 │
                 ▼                                 ▼
       Cache / Persist                      Risk Scoring
                 │                                 │
                 ▼                                 ▼
      UDF Risk Calculation                  Window Analysis
                 │                                 │
                 ▼                                 ▼
       Window Functions                    High-Risk Alerts
                 │
                 ▼
        Spark SQL Analytics
                 │
                 ▼
       Aggregations & Reports
                 │
                 ▼
       Execution Plan / DAG
```

---

# 📂 Project Structure

```text
day30-spark/
│
├── data/
│   ├── patients.csv
│   ├── hospitals.csv
│   ├── historical_vitals.csv
│   ├── medications.csv
│   ├── stream_vitals.txt
│   └── vital_thresholds.csv
│
├── src/
│   └── main/
│       └── scala/
│           ├── Day30HealthcareCapstone.scala
│           └── Day30HealthcareStreaming.scala
│
├── project/
│
├── build.sbt
├── .gitignore
├── README.md
└── INTERVIEW_QUESTIONS.md
```

---

# 📊 Input Datasets

## 1. Patients

`patients.csv`

Contains:

- Patient ID
- Patient name
- Age
- Gender
- Hospital ID
- Department

Example departments include:

- Cardiology
- Neurology
- Orthopedics
- General Medicine

---

## 2. Hospitals

`hospitals.csv`

Contains:

- Hospital ID
- Hospital name
- City
- Total beds

The hospital dataset is used during the hospital enrichment stage.

---

## 3. Historical Vitals

`historical_vitals.csv`

Contains patient health measurements:

- Heart rate
- Temperature
- Oxygen level
- Systolic blood pressure
- Diastolic blood pressure
- Reading timestamp

The historical dataset contains multiple readings for patients across different dates.

---

## 4. Medications

`medications.csv`

Contains:

- Medication ID
- Patient ID
- Medication name
- Dosage
- Start date
- Medication status

This information is joined with patient and vital information.

---

## 5. Streaming Vitals

`stream_vitals.txt`

Contains live-style vital events in the following format:

```text
patient_id,heart_rate,temperature,oxygen_level,systolic_bp,diastolic_bp
```

These events are manually sent through a TCP socket during the streaming demonstration.

---

## 6. Vital Thresholds

`vital_thresholds.csv`

Contains acceptable ranges for:

- Heart rate
- Temperature
- Oxygen level
- Systolic blood pressure
- Diastolic blood pressure

The same threshold concept is used to identify abnormal readings.

---

# ⚙️ Technology Stack

| Technology | Purpose |
|---|---|
| Apache Spark 3.5.3 | Distributed data processing |
| Scala 2.12.18 | Application development |
| Spark SQL | Structured analytics |
| Spark Core | RDD and execution concepts |
| Spark Streaming | Real-time processing |
| SBT | Scala build management |
| CSV | Batch input data |
| TCP Socket | Streaming input |
| Git/GitHub | Version control |

---

# 🔄 Batch Processing Pipeline

The batch application is implemented in:

`Day30HealthcareCapstone.scala`

The pipeline performs the following operations.

## Step 1 — Read Input Data

Spark reads:

- Patients
- Hospitals
- Historical vitals
- Medications

using Spark DataFrames.

---

## Step 2 — Data Cleaning

Historical vital records are cleaned before analytical processing.

The application validates vital values and tracks invalid records using an accumulator.

This creates a clean dataset for downstream processing.

---

# 🔗 Step 3 — Data Integration

The healthcare data is integrated using joins.

The main relationship is:

```text
Historical Vitals
       │
       ▼
Patients
       │
       ├──────────────► Hospitals
       │
       └──────────────► Medications
```

The patient dataset provides the `patient_id` relationship.

The patient information is joined first so that the `hospital_id` becomes available for hospital enrichment.

---

# 🚀 Step 4 — Broadcast Join

The hospital dataset is relatively small compared with the healthcare event data.

Therefore, Spark broadcast functionality is used for the hospital join.

Conceptually:

```text
Large Vital Dataset
        │
        │
        ├───────────────┐
        │               │
        ▼               ▼
Patient Data       Broadcast Hospital Data
        │               │
        └───────┬───────┘
                ▼
        Enriched Healthcare Data
```

Broadcasting allows the smaller dataset to be distributed to executors instead of requiring a large shuffle for the join.

---

# 💾 Step 5 — Cache and Persistence

The enriched healthcare dataset is persisted using:

`MEMORY_AND_DISK`

This demonstrates Spark persistence and allows the processed dataset to be reused across multiple analytical operations.

The project specifically demonstrates:

- Persistence
- Reuse of intermediate results
- Reduced recomputation

---

# 🩺 Step 6 — Vital Status Detection

The project checks the vital measurements against defined threshold ranges.

The following measurements are analyzed:

- Heart rate
- Temperature
- Oxygen level
- Systolic blood pressure
- Diastolic blood pressure

Each record is classified according to whether the vital values are within the expected range.

---

# 🧮 Step 7 — Patient Risk Score

A Spark UDF is used to calculate the patient risk score.

The scoring logic is:

| Abnormal Vital | Score |
|---|---:|
| Heart Rate | +20 |
| Temperature | +20 |
| Oxygen Level | +30 |
| Systolic BP | +15 |
| Diastolic BP | +15 |

The maximum possible score is:

`100`

Risk classification:

| Risk Score | Risk Level |
|---:|---|
| 0–29 | LOW |
| 30–59 | MEDIUM |
| 60–100 | HIGH |

For example, a patient with multiple abnormal vital measurements can receive:

`Risk Score = 100`

and:

`Risk Level = HIGH`

---

# 📈 Step 8 — Window Functions

Window functions are applied at the patient level.

The window is:

```text
PARTITION BY patient_id
ORDER BY reading_time
```

The project uses window functionality to analyze patient readings over time.

The processing includes:

- Previous oxygen reading using `lag`
- Sequential reading identification using `row_number`

This demonstrates how Spark can perform patient-level temporal analysis without collapsing individual records.

---

# 📊 Step 9 — Aggregations

The batch pipeline performs analytical aggregations by:

- Department
- Hospital

These aggregations demonstrate Spark's distributed aggregation capabilities.

The results provide summarized healthcare information at organizational levels.

---

# 🗃️ Step 10 — Spark SQL

A temporary view is created from the enriched healthcare dataset.

Spark SQL is then used to generate an analytical report.

This demonstrates the use of:

- SQL queries
- DataFrames
- Temporary views
- Aggregation
- Filtering
- Structured analytics

---

# 🔢 Step 11 — Partition Tuning

The project also demonstrates repartitioning using:

```text
repartition(4, department)
```

This explicitly changes the partition distribution based on department.

Partition tuning is demonstrated to show how Spark data distribution can be controlled for downstream processing.

---

# ➕ Step 12 — Accumulator

A Spark accumulator is used to count invalid vital records.

The accumulator provides a distributed counter that can be updated during Spark processing.

This demonstrates how Spark accumulators can be used for:

- Monitoring
- Counters
- Data-quality statistics

---

# 🔍 Spark Execution Plan

The batch application calls:

```text
explain("formatted")
```

to inspect Spark's physical execution plan.

The execution plan demonstrated operators including:

- `BroadcastHashJoin`
- `BroadcastExchange`
- `Exchange`
- `HashAggregate`
- `Sort`
- `AdaptiveSparkPlan`
- `InMemoryTableScan`
- `InMemoryRelation`

These operators demonstrate how Spark transforms the logical operations into an executable physical plan.

---

# 🧠 DAG, Lineage and Execution

The project also demonstrates important Spark execution concepts.

## DAG

Spark creates a Directed Acyclic Graph representing the sequence of transformations and actions.

```text
Read Data
   │
   ▼
Clean
   │
   ▼
Join
   │
   ▼
Enrich
   │
   ▼
Transform
   │
   ▼
Aggregate
   │
   ▼
Action
```

---

## Lineage

Spark maintains lineage information for transformations.

If an intermediate partition is lost, Spark can use lineage information to recompute the required data instead of requiring the entire application to restart.

---

## Stages

Spark divides jobs into stages.

Shuffle boundaries are important because operations that require data movement between partitions can create new stages.

Examples include:

- Aggregations
- Repartitioning
- Certain joins

---

## Executors

Executors perform the actual computation for Spark tasks.

They:

- Execute tasks
- Store cached data
- Process partitions
- Return results to the driver

---

# 🖥️ YARN Concept

The project also covers Spark execution concepts in a YARN environment.

A typical deployment consists of:

```text
Spark Application
       │
       ▼
    Driver
       │
       ▼
YARN ResourceManager
       │
       ▼
NodeManagers
       │
       ├── Executor
       ├── Executor
       └── Executor
```

The ResourceManager manages cluster resources while executors perform Spark tasks.

The current capstone was executed locally for development and demonstration, while YARN architecture and execution concepts were studied as part of the Spark training.

---

# ⚡ Real-Time Streaming Pipeline

The streaming application is implemented in:

`Day30HealthcareStreaming.scala`

The streaming pipeline uses Spark Streaming with a TCP socket source.

Architecture:

```text
TCP Socket
    │
    ▼
Raw Vital Events
    │
    ▼
5-Second Micro-Batches
    │
    ▼
Parsing & Validation
    │
    ▼
Valid Vital Events
    │
    ▼
Abnormal Vital Detection
    │
    ├───────────────► Live Vital Events
    │
    ├───────────────► Abnormal Vital Alerts
    │
    ├───────────────► Patient Risk
    │
    └───────────────► 10-Second Window
                              │
                              ▼
                       High-Risk Alerts
```

---

# ⏱️ Streaming Configuration

The streaming application uses:

- Socket source: `localhost:9999`
- Batch interval: `5 seconds`
- Window duration: `10 seconds`
- Window slide: `5 seconds`

Checkpointing is configured because the streaming application uses a stateful window operation.

---

# 🧪 Streaming Validation

Each incoming event is parsed into a healthcare vital event.

The event contains:

- Patient ID
- Heart rate
- Temperature
- Oxygen level
- Systolic BP
- Diastolic BP

Invalid events are counted using a Spark accumulator.

Valid events continue through the streaming pipeline.

---

# 🚨 Abnormal Vital Detection

Each valid streaming event is compared with the vital thresholds.

The application checks:

- Heart rate
- Temperature
- Oxygen level
- Systolic blood pressure
- Diastolic blood pressure

If one or more measurements are abnormal, the event is sent to the abnormal-vital processing path.

---

# 🧮 Streaming Risk Scoring

The same risk-scoring concept is applied to abnormal streaming events.

Example observed output:

```text
P006 | Risk Score=100 | Risk Level=HIGH
```

Another observed event:

```text
P003 | Risk Score=100 | Risk Level=HIGH
```

This demonstrates real-time patient risk identification.

---

# 🪟 Streaming Window Operation

The application maintains a 10-second window of abnormal readings.

The window is updated every 5 seconds.

Example observed output:

```text
P006 | Abnormal readings in window=1
P003 | Abnormal readings in window=1
```

As events leave the window, the counts decrease.

This demonstrates stateful window processing.

---

# 🚨 High-Risk Alert Processing

The streaming application also checks repeated abnormal readings inside the window.

The purpose is to identify patients generating repeated abnormal vital events within the monitoring period.

This provides the basis for real-time healthcare alerting.

---

# 🧪 Actual Streaming Test

The streaming pipeline was tested using live socket input.

Example events included:

```text
P001,78,36.7,98,120,80
P003,108,38.0,93,142,91
P006,112,38.6,90,152,96
P003,115,38.5,92,148,94
P006,118,39.0,89,155,98
```

The application successfully produced:

- Live vital events
- Abnormal vital alerts
- Patient risk scores
- HIGH risk classifications
- 10-second window results

Observed examples included:

```text
ABNORMAL | P006 | HR=112.0 | TEMP=38.6 | O2=90.0 | BP=152.0/96.0

P006 | Risk Score=100 | Risk Level=HIGH

ABNORMAL | P003 | HR=115.0 | TEMP=38.5 | O2=92.0 | BP=148.0/94.0

P003 | Risk Score=100 | Risk Level=HIGH
```

The streaming test completed successfully.

---

# 📌 Spark Concepts Demonstrated

This capstone brings together the major Spark concepts covered during the training:

| Concept | Demonstrated |
|---|---|
| DataFrames | ✅ |
| Spark SQL | ✅ |
| Joins | ✅ |
| Broadcast Join | ✅ |
| Aggregations | ✅ |
| Window Functions | ✅ |
| UDF | ✅ |
| Cache/Persist | ✅ |
| Accumulator | ✅ |
| Repartition | ✅ |
| DAG | ✅ |
| Lineage | ✅ |
| Stages | ✅ |
| Shuffle concepts | ✅ |
| Physical Execution Plan | ✅ |
| Adaptive Query Execution plan | ✅ |
| Spark Streaming | ✅ |
| Micro-batches | ✅ |
| Stateful Window | ✅ |
| Streaming Risk Detection | ✅ |
| Real-time Alerts | ✅ |
| YARN Concepts | ✅ |

---

# 📸 Project Evidence

Only the important execution evidence is retained instead of keeping every intermediate Spark output screenshot.

Recommended evidence includes:

1. Batch pipeline completion
2. Patient risk results
3. Vital-status results
4. Department/hospital aggregations
5. Window-function results
6. Spark SQL / optimization results
7. Physical execution plan
8. Streaming live alerts and risk results

The README documents the complete implementation so that the project does not depend on dozens of screenshots.

---

# 📈 Optimization Techniques Used

The project demonstrates several Spark optimization concepts.

### Broadcast Join

Used for the relatively small hospital dataset.

### Persistence

The enriched dataset is persisted using `MEMORY_AND_DISK`.

### Repartitioning

Department-based repartitioning is demonstrated.

### Spark SQL

Structured queries are used for analytical processing.

### Window Functions

Patient-level temporal analysis is performed using Spark windows.

### Physical Plan Analysis

The formatted execution plan is inspected to understand Spark's actual execution strategy.

---

# 🏆 Final Outcome

The Day 30 capstone successfully combines batch analytics and real-time monitoring into one healthcare-oriented Spark application.

The batch pipeline successfully performs:

- Data ingestion
- Data cleaning
- Multi-dataset integration
- Broadcast join
- Persistence
- Risk scoring
- Window analysis
- Aggregations
- Spark SQL
- Partition tuning
- Accumulator-based monitoring
- Physical plan analysis

The streaming pipeline successfully performs:

- Socket-based ingestion
- Micro-batch processing
- Event validation
- Abnormal vital detection
- Real-time risk scoring
- Stateful window processing
- High-risk monitoring

The project demonstrates the practical use of Apache Spark for a complete data-engineering workflow involving both historical analytics and real-time healthcare monitoring.

---

# ⭐ Project Status

**Day 30 Smart Healthcare Monitoring & Patient Risk Analytics Platform — COMPLETED ✅**
