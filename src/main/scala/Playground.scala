import org.apache.log4j.{Level, Logger}

object Playground extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  override val appName: String = "Playground"

  val bikeSharingDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/bike_sharing.csv")
    .select("Hour",
      "TEMPERATURE",
      "HUMIDITY", "WIND_SPEED")

  bikeSharingDF.show(3)
}