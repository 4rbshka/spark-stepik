import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object Playground extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  override val appName: String = "Playground"

  val mallCustomersDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/mall_customers.csv")
    .withColumn("Age", col("Age") + 2)

  val incomeDF = mallCustomersDF
    .withColumn("gender_code",
      when(col("Gender") === "Male", 1)
        .otherwise(0)
    )
    .filter("Age BETWEEN 30 AND 35")
    .groupBy("Gender", "Age", "gender_code")
    .agg(
      round(avg("Annual Income (k$)"), 1).as("AVG_income")
    )
    .orderBy("gender_code", "Age")

  incomeDF.write
    .mode(SaveMode.Overwrite)
    .save("resources/data/customers")
}