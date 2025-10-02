import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

object Playground extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  override val appName: String = "Playground"

  val bikeSharingDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/bike_sharing.csv")

  val res = bikeSharingDF
    .withColumn(
      "is_workday",
      when(col("HOLIDAY") === "Holiday" && col("FUNCTIONING_DAY") === "No", 0)
    .otherwise(1)
    )
    .dropDuplicates("HOLIDAY", "FUNCTIONING_DAY", "is_workday")
    .show()
}