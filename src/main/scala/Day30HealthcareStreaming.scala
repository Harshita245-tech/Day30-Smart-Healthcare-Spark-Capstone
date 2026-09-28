import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day30HealthcareStreaming {

  case class VitalEvent(
      patientId: String,
      heartRate: Double,
      temperature: Double,
      oxygenLevel: Double,
      systolicBP: Double,
      diastolicBP: Double
  )

  def main(args: Array[String]): Unit = {

    println()
    println("======================================================")
    println(" DAY 30 - REAL-TIME HEALTHCARE STREAMING")
    println(" Patient Vital Monitoring & Risk Alerts")
    println("======================================================")
    println()

    val conf = new SparkConf()
      .setAppName("Day30HealthcareStreaming")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("/tmp/day30-healthcare-streaming-checkpoint")

    ssc.sparkContext.setLogLevel("ERROR")

    // ======================================================
    // VITAL THRESHOLDS
    // ======================================================

    val thresholds = Map(
      "heart_rate_min" -> 60.0,
      "heart_rate_max" -> 100.0,
      "temperature_min" -> 36.0,
      "temperature_max" -> 37.5,
      "oxygen_min" -> 95.0,
      "systolic_min" -> 90.0,
      "systolic_max" -> 140.0,
      "diastolic_min" -> 60.0,
      "diastolic_max" -> 90.0
    )

    val broadcastThresholds: Broadcast[Map[String, Double]] =
      ssc.sparkContext.broadcast(thresholds)

    // ======================================================
    // ACCUMULATOR
    // ======================================================

    val invalidEventCounter =
      ssc.sparkContext.longAccumulator("InvalidVitalEvents")

    // ======================================================
    // SOCKET STREAM
    // ======================================================

    val rawStream =
      ssc.socketTextStream("localhost", 9999)

    // ======================================================
    // PARSE EVENTS
    // ======================================================

    val parsedStream =
      rawStream.map { line =>

        val parts = line.split(",")

        try {

          if (parts.length != 6) {
            throw new IllegalArgumentException(
              "Invalid number of fields"
            )
          }

          VitalEvent(
            parts(0).trim,
            parts(1).trim.toDouble,
            parts(2).trim.toDouble,
            parts(3).trim.toDouble,
            parts(4).trim.toDouble,
            parts(5).trim.toDouble
          )

        } catch {

          case _: Exception =>
            invalidEventCounter.add(1)
            null
        }
      }

    val validStream =
      parsedStream.filter(event => event != null)

    // ======================================================
    // ABNORMAL VITAL DETECTION
    // ======================================================

    val abnormalStream =
      validStream.filter { event =>

        val t = broadcastThresholds.value

        val heartRateAbnormal =
          event.heartRate < t("heart_rate_min") ||
          event.heartRate > t("heart_rate_max")

        val temperatureAbnormal =
          event.temperature < t("temperature_min") ||
          event.temperature > t("temperature_max")

        val oxygenAbnormal =
          event.oxygenLevel < t("oxygen_min")

        val systolicAbnormal =
          event.systolicBP < t("systolic_min") ||
          event.systolicBP > t("systolic_max")

        val diastolicAbnormal =
          event.diastolicBP < t("diastolic_min") ||
          event.diastolicBP > t("diastolic_max")

        Seq(
          heartRateAbnormal,
          temperatureAbnormal,
          oxygenAbnormal,
          systolicAbnormal,
          diastolicAbnormal
        ).exists(identity)
      }

    // ======================================================
    // PATIENT RISK SCORE
    // ======================================================

    val riskStream =
      abnormalStream.map { event =>

        val t = broadcastThresholds.value

        var score = 0

        val heartRateAbnormal =
          event.heartRate < t("heart_rate_min") ||
          event.heartRate > t("heart_rate_max")

        val temperatureAbnormal =
          event.temperature < t("temperature_min") ||
          event.temperature > t("temperature_max")

        val oxygenAbnormal =
          event.oxygenLevel < t("oxygen_min")

        val systolicAbnormal =
          event.systolicBP < t("systolic_min") ||
          event.systolicBP > t("systolic_max")

        val diastolicAbnormal =
          event.diastolicBP < t("diastolic_min") ||
          event.diastolicBP > t("diastolic_max")

        if (heartRateAbnormal) {
          score += 20
        }

        if (temperatureAbnormal) {
          score += 20
        }

        if (oxygenAbnormal) {
          score += 30
        }

        if (systolicAbnormal) {
          score += 15
        }

        if (diastolicAbnormal) {
          score += 15
        }

        val riskLevel =
          if (score >= 60) {
            "HIGH"
          } else if (score >= 30) {
            "MEDIUM"
          } else {
            "LOW"
          }

        (
          event.patientId,
          score,
          riskLevel,
          event
        )
      }

    // ======================================================
    // 10-SECOND WINDOW
    // ======================================================

    val abnormalCountWindow =
      abnormalStream
        .map(event => (event.patientId, 1))
        .reduceByKeyAndWindow(
          (a: Int, b: Int) => a + b,
          (a: Int, b: Int) => a - b,
          Seconds(10),
          Seconds(5)
        )

    // ======================================================
    // LIVE EVENTS
    // ======================================================

    validStream.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("========== LIVE VITAL EVENTS ==========")

        rdd.foreach { event =>

          println(
            s"${event.patientId} | " +
            s"HR=${event.heartRate} | " +
            s"TEMP=${event.temperature} | " +
            s"O2=${event.oxygenLevel} | " +
            s"BP=${event.systolicBP}/${event.diastolicBP}"
          )
        }
      }
    }

    // ======================================================
    // ABNORMAL ALERTS
    // ======================================================

    abnormalStream.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("========== ABNORMAL VITAL ALERT ==========")

        rdd.foreach { event =>

          println(
            s"ABNORMAL | ${event.patientId} | " +
            s"HR=${event.heartRate} | " +
            s"TEMP=${event.temperature} | " +
            s"O2=${event.oxygenLevel} | " +
            s"BP=${event.systolicBP}/${event.diastolicBP}"
          )
        }
      }
    }

    // ======================================================
    // RISK SCORE
    // ======================================================

    riskStream.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("========== PATIENT RISK ==========")

        rdd.foreach {
          case (
                patientId,
                score,
                riskLevel,
                event
              ) =>

            println(
              s"$patientId | " +
              s"Risk Score=$score | " +
              s"Risk Level=$riskLevel"
            )
        }
      }
    }

    // ======================================================
    // WINDOW ALERTS
    // ======================================================

    abnormalCountWindow.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("========== 10-SECOND WINDOW ==========")

        rdd.foreach {
          case (patientId, count) =>

            println(
              s"$patientId | " +
              s"Abnormal readings in window=$count"
            )

            if (count >= 2) {

              println(
                s"HIGH-RISK ALERT | $patientId | " +
                s"Repeated abnormal readings detected"
              )
            }
        }
      }
    }

    // ======================================================
    // START STREAMING
    // ======================================================

    ssc.start()

    println()
    println("======================================================")
    println(" STREAMING STARTED SUCCESSFULLY")
    println(" Socket : localhost:9999")
    println(" Batch Interval : 5 seconds")
    println(" Window : 10 seconds")
    println("======================================================")
    println()

    ssc.awaitTermination()
  }
}
