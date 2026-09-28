import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object Day30HealthcareCapstone {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day30SmartHealthcareCapstone")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // ============================================================
    // DAY 30 - SMART HEALTHCARE MONITORING & PATIENT RISK ANALYTICS
    // ============================================================

    println()
    println("======================================================")
    println(" DAY 30 - SMART HEALTHCARE CAPSTONE")
    println(" SMART HEALTHCARE MONITORING & PATIENT RISK ANALYTICS")
    println("======================================================")
    println()

    // ============================================================
    // FILE PATHS
    // ============================================================

    val patientsPath = "data/patients.csv"
    val hospitalsPath = "data/hospitals.csv"
    val vitalsPath = "data/historical_vitals.csv"
    val medicationsPath = "data/medications.csv"

    // ============================================================
    // 1. READ DATA
    // ============================================================

    val patients = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(patientsPath)

    val hospitals = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(hospitalsPath)

    val historicalVitals = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(vitalsPath)
      .withColumn(
        "reading_time",
        to_timestamp($"reading_time")
      )

    val medications = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(medicationsPath)

    // ============================================================
    // 2. CLEAN HISTORICAL VITALS
    // ============================================================

    val cleanVitals = historicalVitals
      .filter($"patient_id".isNotNull)
      .filter($"reading_time".isNotNull)
      .filter($"heart_rate".isNotNull)
      .filter($"temperature".isNotNull)
      .filter($"oxygen_level".isNotNull)
      .filter($"systolic_bp".isNotNull)
      .filter($"diastolic_bp".isNotNull)
      .filter($"heart_rate" > 0)
      .filter($"temperature" > 0)
      .filter($"oxygen_level" > 0)
      .dropDuplicates(
        "patient_id",
        "reading_time"
      )

    // ============================================================
    // 3. PATIENT + VITAL JOIN
    // ============================================================

    val patientVitals = cleanVitals
      .join(
        patients,
        Seq("patient_id"),
        "inner"
      )
      .select(
        $"patient_id",
        $"patient_name",
        $"age",
        $"gender",
        $"hospital_id",
        $"department",
        $"reading_time",
        $"heart_rate",
        $"temperature",
        $"oxygen_level",
        $"systolic_bp",
        $"diastolic_bp"
      )

    // ============================================================
    // 4. BROADCAST HOSPITAL JOIN
    // ============================================================

    val enrichedVitals = patientVitals
      .join(
        broadcast(hospitals),
        patientVitals("hospital_id") === hospitals("hospital_id"),
        "left"
      )
      .select(
        patientVitals("*"),
        hospitals("hospital_name"),
        hospitals("city")
      )

    // ============================================================
    // 5. PATIENT + MEDICATION JOIN
    // ============================================================

    val completePatientData = enrichedVitals
      .join(
        medications,
        Seq("patient_id"),
        "left"
      )
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Trigger persistence
    completePatientData.count()

    // ============================================================
    // 6. VITAL STATUS
    // ============================================================

    val vitalStatus = completePatientData
      .withColumn(
        "vital_status",
        when(
          ($"heart_rate" < 60 || $"heart_rate" > 100) ||
          ($"temperature" < 36.0 || $"temperature" > 37.5) ||
          ($"oxygen_level" < 95) ||
          ($"systolic_bp" < 90 || $"systolic_bp" > 140) ||
          ($"diastolic_bp" < 60 || $"diastolic_bp" > 90),
          "ABNORMAL"
        ).otherwise("NORMAL")
      )

    // ============================================================
    // 7. UDF - PATIENT RISK SCORE
    // ============================================================

    val patientRiskScore = udf(
      (
        heartRate: Double,
        temperature: Double,
        oxygen: Double,
        systolic: Double,
        diastolic: Double
      ) => {

        var score = 0

        if (heartRate < 60 || heartRate > 100) {
          score += 20
        }

        if (temperature < 36.0 || temperature > 37.5) {
          score += 20
        }

        if (oxygen < 95) {
          score += 30
        }

        if (systolic < 90 || systolic > 140) {
          score += 15
        }

        if (diastolic < 60 || diastolic > 90) {
          score += 15
        }

        score
      }
    )

    val riskData = vitalStatus
      .withColumn(
        "risk_score",
        patientRiskScore(
          $"heart_rate",
          $"temperature",
          $"oxygen_level",
          $"systolic_bp",
          $"diastolic_bp"
        )
      )
      .withColumn(
        "risk_level",
        when($"risk_score" >= 60, "HIGH")
          .when($"risk_score" >= 30, "MEDIUM")
          .otherwise("LOW")
      )

    // ============================================================
    // 8. WINDOW FUNCTIONS
    // ============================================================

    val patientWindow =
      Window
        .partitionBy("patient_id")
        .orderBy("reading_time")

    val windowAnalytics = riskData
      .withColumn(
        "previous_oxygen",
        lag("oxygen_level", 1).over(patientWindow)
      )
      .withColumn(
        "reading_number",
        row_number().over(patientWindow)
      )

    // ============================================================
    // 9. DEPARTMENT AGGREGATION
    // ============================================================

    val departmentReport = riskData
      .groupBy("department")
      .agg(
        count("*").alias("total_readings"),
        round(avg("heart_rate"), 2).alias("avg_heart_rate"),
        round(avg("oxygen_level"), 2).alias("avg_oxygen"),
        round(avg("temperature"), 2).alias("avg_temperature"),
        sum(
          when(
            $"risk_level" === "HIGH",
            1
          ).otherwise(0)
        ).alias("high_risk_cases")
      )
      .orderBy(desc("high_risk_cases"))

    // ============================================================
    // 10. HOSPITAL AGGREGATION
    // ============================================================

    val hospitalReport = riskData
      .groupBy(
        "hospital_id",
        "hospital_name",
        "city"
      )
      .agg(
        count("*").alias("total_readings"),
        round(avg("heart_rate"), 2).alias("avg_heart_rate"),
        round(avg("oxygen_level"), 2).alias("avg_oxygen"),
        sum(
          when(
            $"risk_level" === "HIGH",
            1
          ).otherwise(0)
        ).alias("high_risk_cases")
      )
      .orderBy(desc("high_risk_cases"))

    // ============================================================
    // 11. SPARK SQL
    // ============================================================

    riskData.createOrReplaceTempView(
      "patient_risk"
    )

    val sqlReport = spark.sql(
      """
        SELECT
          hospital_name,
          department,
          risk_level,
          COUNT(*) AS readings,
          ROUND(AVG(oxygen_level), 2) AS avg_oxygen,
          ROUND(AVG(heart_rate), 2) AS avg_heart_rate
        FROM patient_risk
        GROUP BY
          hospital_name,
          department,
          risk_level
        ORDER BY readings DESC
      """
    )

    // ============================================================
    // 12. PARTITION TUNING
    // ============================================================

    val partitionedRiskData =
      riskData.repartition(
        4,
        $"department"
      )

    val partitionCount =
      partitionedRiskData.rdd.getNumPartitions

    // ============================================================
    // 13. ACCUMULATOR
    // ============================================================

    val invalidVitalCounter =
      spark.sparkContext
        .longAccumulator("InvalidVitalRecords")

    riskData.foreach { row =>

      val oxygenValue =
        row.getAs[Number]("oxygen_level")

      val oxygen =
        oxygenValue.doubleValue()

      if (oxygen <= 0.0 || oxygen > 100.0) {
        invalidVitalCounter.add(1)
      }
    }

    // ============================================================
    // 14. FINAL OUTPUT
    // ============================================================

    println()
    println("======================================================")
    println("              DAY 30 FINAL OUTPUT")
    println("======================================================")

    println()
    println("DATA SUMMARY")
    println("----------------------------------------")

    println(
      s"Patients            : ${patients.count()}"
    )

    println(
      s"Hospitals           : ${hospitals.count()}"
    )

    println(
      s"Historical vitals   : ${historicalVitals.count()}"
    )

    println(
      s"Clean vital records : ${cleanVitals.count()}"
    )

    println(
      s"Medications         : ${medications.count()}"
    )

    println(
      s"Enriched records    : ${completePatientData.count()}"
    )

    // ============================================================
    // PATIENT RISK
    // ============================================================

    println()
    println("PATIENT RISK ANALYTICS")
    println("----------------------------------------")

    riskData
      .select(
        "patient_id",
        "patient_name",
        "department",
        "risk_score",
        "risk_level"
      )
      .orderBy(
        desc("risk_score"),
        asc("patient_id")
      )
      .show(
        20,
        false
      )

    // ============================================================
    // VITAL STATUS
    // ============================================================

    println()
    println("VITAL STATUS SUMMARY")
    println("----------------------------------------")

    vitalStatus
      .groupBy("vital_status")
      .count()
      .orderBy(desc("count"))
      .show(false)

    // ============================================================
    // DEPARTMENT REPORT
    // ============================================================

    println()
    println("DEPARTMENT ANALYTICS")
    println("----------------------------------------")

    departmentReport.show(false)

    // ============================================================
    // HOSPITAL REPORT
    // ============================================================

    println()
    println("HOSPITAL ANALYTICS")
    println("----------------------------------------")

    hospitalReport.show(false)

    // ============================================================
    // WINDOW REPORT
    // ============================================================

    println()
    println("WINDOW ANALYTICS")
    println("----------------------------------------")

    windowAnalytics
      .select(
        "patient_id",
        "patient_name",
        "reading_time",
        "oxygen_level",
        "previous_oxygen",
        "reading_number",
        "risk_level"
      )
      .orderBy(
        "patient_id",
        "reading_time"
      )
      .show(
        20,
        false
      )

    // ============================================================
    // SQL REPORT
    // ============================================================

    println()
    println("SPARK SQL REPORT")
    println("----------------------------------------")

    sqlReport.show(false)

    // ============================================================
    // OPTIMIZATION SUMMARY
    // ============================================================

    println()
    println("OPTIMIZATION SUMMARY")
    println("----------------------------------------")

    println(
      s"Partitions after tuning : $partitionCount"
    )

    println(
      s"Invalid vital records   : ${invalidVitalCounter.value}"
    )

    println(
      "Hospital master         : Broadcast Join"
    )

    println(
      "Enriched dataset        : MEMORY_AND_DISK"
    )

    println(
      "Window functions        : LAG + ROW_NUMBER"
    )

    println(
      "Custom business logic   : Patient Risk UDF"
    )

    println(
      "Aggregation             : Department + Hospital"
    )

    // ============================================================
    // EXECUTION PLAN
    // ============================================================

    println()
    println("EXECUTION PLAN")
    println("----------------------------------------")

    sqlReport.explain("formatted")

    // ============================================================
    // SPARK CONCEPTS
    // ============================================================

    println()
    println("SPARK CONCEPTS")
    println("----------------------------------------")

    println(
      "DAG        : Directed Acyclic Graph"
    )

    println(
      "Lineage    : Transformation dependency chain"
    )

    println(
      "Stages     : Created around shuffle boundaries"
    )

    println(
      "Executors  : Execute tasks and store cached data"
    )

    println(
      "Broadcast  : Small hospital reference dataset"
    )

    println(
      "Accumulator: Invalid vital record counter"
    )

    println(
      "Persist    : MEMORY_AND_DISK"
    )

    println(
      "Partition  : Department-based repartitioning"
    )

    println(
      "Window     : Patient-level reading history"
    )

    println(
      "UDF        : Patient risk score"
    )

    println(
      "SQL        : Healthcare analytics report"
    )

    println(
      "YARN       : Production cluster deployment"
    )

    // ============================================================
    // STREAMING READY
    // ============================================================

    println()
    println("STREAMING COMPONENT")
    println("----------------------------------------")

    println(
      "Input file : data/stream_vitals.txt"
    )

    println(
      "Streaming design : Live patient vital monitoring"
    )

    println(
      "Broadcast : Vital threshold reference"
    )

    println(
      "Accumulator : Invalid event counter"
    )

    println(
      "Window : Repeated abnormal readings"
    )

    println(
      "Alert : HIGH patient risk"
    )

    // ============================================================
    // FINAL STATUS
    // ============================================================

    println()
    println("======================================================")
    println("       DAY 30 BATCH PIPELINE COMPLETED SUCCESSFULLY")
    println("======================================================")
    println()

    completePatientData.unpersist()

    spark.stop()
  }
}
