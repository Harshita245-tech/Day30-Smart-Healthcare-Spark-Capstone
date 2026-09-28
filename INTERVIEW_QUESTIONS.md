# 🎯 Day 30 — Smart Healthcare Monitoring & Patient Risk Analytics
# Apache Spark Interview Questions & Answers

## 1. What is the objective of your Day 30 project?

**Answer:**

My Day 30 project is a Smart Healthcare Monitoring and Patient Risk Analytics Platform using Apache Spark and Scala.

It combines batch and real-time processing. In batch processing, I analyze historical patient vitals, patient details, hospitals and medications. In streaming, I process live vital events through a socket, detect abnormal readings, calculate risk scores and generate high-risk alerts.

---

## 2. Why did you use Apache Spark for this project?

**Answer:**

I used Apache Spark because it provides distributed processing for large datasets and supports both batch and streaming workloads.

In my project, I used Spark DataFrames, Spark SQL, joins, aggregations, window functions, UDFs, caching, broadcast joins and Spark Streaming.

---

## 3. What are the main datasets used in your project?

**Answer:**

I used six main datasets:

- `patients.csv`
- `hospitals.csv`
- `historical_vitals.csv`
- `medications.csv`
- `stream_vitals.txt`
- `vital_thresholds.csv`

The historical files are used for batch processing, while the streaming vital file provides sample real-time events.

---

## 4. How did you integrate the patient, hospital and medication data?

**Answer:**

First, I joined the historical vital records with the patient dataset using `patient_id`.

This gives me patient information and `hospital_id`.

Then I use `hospital_id` to enrich the data with hospital information and use `patient_id` again to join medication information.

So the basic flow is:

Vitals → Patients → Hospitals + Medications.

---

## 5. Why did you use a Broadcast Join?

**Answer:**

I used a broadcast join for the hospital dataset because it is relatively small compared with the healthcare event data.

Spark can broadcast the smaller dataset to the executors, which avoids unnecessary data movement for that join.

In my physical execution plan, I could see operators such as `BroadcastHashJoin` and `BroadcastExchange`.

---

## 6. What is the difference between a normal join and a broadcast join?

**Answer:**

In a normal join, Spark may need to shuffle data across partitions so that matching keys are colocated.

In a broadcast join, Spark sends the smaller dataset to the executors and performs the join locally with the larger dataset.

So broadcast joins can reduce shuffle when one side of the join is small enough.

---

## 7. Why did you use Cache or Persist in this project?

**Answer:**

I persisted the enriched healthcare dataset using `MEMORY_AND_DISK`.

The reason is that the enriched dataset is reused by multiple analytical operations such as risk calculation, window processing and aggregations.

Persistence avoids recomputing the same transformations every time the dataset is used.

---

## 8. What is the purpose of the UDF in your project?

**Answer:**

I used a User Defined Function to calculate the patient risk score.

The UDF checks whether the patient's vital measurements are abnormal and assigns scores based on those conditions.

The scoring is:

- Heart rate: 20
- Temperature: 20
- Oxygen level: 30
- Systolic BP: 15
- Diastolic BP: 15

The maximum score is 100.

---

## 9. How is the patient risk level calculated?

**Answer:**

After calculating the risk score, I classify the patient into three levels.

A score from 0 to 29 is LOW.

A score from 30 to 59 is MEDIUM.

A score from 60 to 100 is HIGH.

For example, in my streaming test, P006 received a risk score of 100 and was classified as HIGH risk.

---

## 10. Why did you use Window Functions?

**Answer:**

I used window functions to analyze patient readings over time without grouping the records into a single row.

The window is partitioned by `patient_id` and ordered by `reading_time`.

I used functions such as `lag` to access the previous oxygen reading and `row_number` to identify the sequence of readings for each patient.

---

## 11. What is the difference between groupBy and a Window Function?

**Answer:**

`groupBy` combines multiple rows into aggregated results, so the individual rows are generally not retained.

A window function performs calculations across related rows while keeping the original rows.

In my project, I used groupBy for department and hospital-level summaries and window functions for patient-level time-based analysis.

---

## 12. What is an Accumulator and how did you use it?

**Answer:**

An accumulator is a Spark mechanism used for aggregating values from distributed tasks.

In my project, I used an accumulator to count invalid vital records during data validation.

This gives me a simple data-quality counter while Spark processes the records.

---

## 13. Why did you use repartition?

**Answer:**

I used repartition to demonstrate partition tuning.

In the project, I used department-based repartitioning with four partitions.

Repartition can redistribute data across partitions and can be useful when we want better data distribution for downstream processing.

However, repartition itself can cause a shuffle, so it should be used when there is a real processing requirement.

---

## 14. What did you observe in the Spark physical execution plan?

**Answer:**

I inspected the physical plan using `explain("formatted")`.

The plan showed operators including:

- `BroadcastHashJoin`
- `BroadcastExchange`
- `Exchange`
- `HashAggregate`
- `Sort`
- `AdaptiveSparkPlan`
- `InMemoryTableScan`
- `InMemoryRelation`

These operators helped me understand how Spark converted my logical operations into an executable physical plan.

---

## 15. What is an Exchange in Spark?

**Answer:**

An Exchange represents data redistribution between partitions.

It commonly appears when Spark needs to perform a shuffle.

For example, operations such as repartitioning and certain aggregations can require data to move between partitions.

This redistribution can also create stage boundaries in the Spark execution process.

---

## 16. What is a DAG in Spark?

**Answer:**

DAG stands for Directed Acyclic Graph.

Spark creates a DAG representing the sequence of transformations and actions in an application.

Spark uses this DAG to divide the work into stages and tasks and then execute them efficiently.

In my project, the flow includes reading data, cleaning, joining, enriching, transforming, aggregating and finally performing actions.

---

## 17. What is Spark lineage?

**Answer:**

Lineage is the record of transformations used to create a dataset.

Spark uses lineage for fault tolerance.

If a partition is lost, Spark can recompute that partition using the transformation history instead of recomputing the entire application from the beginning.

---

## 18. What did you implement in the streaming part?

**Answer:**

For streaming, I used Spark Streaming with a TCP socket source on `localhost:9999`.

The application processes data in 5-second micro-batches.

It performs:

- Event parsing
- Validation
- Abnormal vital detection
- Risk scoring
- Patient risk classification
- 10-second window processing
- High-risk monitoring

During testing, I sent patient vital events through the socket and received live alerts in the Spark application.

---

## 19. Why does the streaming application require checkpointing?

**Answer:**

My streaming application uses a stateful window operation with `reduceByKeyAndWindow`.

Stateful streaming operations require checkpointing so Spark can store the required state and recover it if necessary.

I configured a checkpoint directory for the streaming application before starting it.

---

## 20. Explain your complete Day 30 project flow.

**Answer:**

My complete project has two major pipelines.

For batch processing, I read historical healthcare data, clean it, join patient information with hospital and medication data, broadcast the smaller hospital dataset, persist the enriched data, calculate risk scores using a UDF, apply window functions, perform aggregations and Spark SQL analysis, and inspect the physical execution plan.

For streaming, I receive live patient vital events through a TCP socket, process them in 5-second micro-batches, validate the events, detect abnormal vitals, calculate risk scores and use a 10-second window to monitor repeated abnormal readings.

So the project demonstrates both historical healthcare analytics and real-time patient monitoring using Apache Spark.
