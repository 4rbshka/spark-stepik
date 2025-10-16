import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Playground extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  private def createSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }

  val appName: String = "Playground"
  lazy val spark = createSession(appName)

  import spark.implicits._

  case class Cars(
                   id: Int,
                   price: Int,
                   brand: String,
                   type_car: String,
                   mileage: Double,
                   color: String,
                   date_of_purchase: String
                 )

  case class CarsAge(
                      id: Int,
                      price: Int,
                      brand: String,
                      type_car: String,
                      mileage: Double,
                      color: String,
                      date_of_purchase: String,
                      avg_mileage: Double,
                      years_since_purchase: Int
                    )

  def readCarsData(filePath: String): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(filePath)
  }

  def normalizeColumnNames: DataFrame => DataFrame = { df =>
    df.withColumnRenamed("type", "type_car")
  }

  def unifyDateFormats: DataFrame => DataFrame = { df =>
    df.withColumn("date_of_purchase",
      date_format(
        coalesce(
          to_date(col("date_of_purchase"), "yyyy MM dd"),
          to_date(col("date_of_purchase"), "yyyy-MM-dd"),
          to_date(col("date_of_purchase"), "yyyy MMM dd"),
          to_date(col("date_of_purchase"), "MM/dd/yyyy")
        ),
        "yyyy-MM-dd"
      ))
  }

  def fillMissingMileage: DataFrame => DataFrame = { df =>
    df.na.fill(0.0, List("mileage"))
  }

  def calculateAverageMileage: Dataset[Cars] => Double = { ds =>
    ds.map(_.mileage).reduce(_ + _) / ds.count()
  }

  def calculateCarAge(dateOfPurchase: String, currentDate: String = "2025-10-16"): Int = {
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val currentMillis = sdf.parse(currentDate).getTime
    val purchaseMillis = sdf.parse(dateOfPurchase).getTime

    val daysSincePurchase = (currentMillis - purchaseMillis) / (1000 * 60 * 60 * 24)
    (daysSincePurchase / 365.25).toInt
  }

  def addCarAgeInfo(avgMileage: Double): Dataset[Cars] => Dataset[CarsAge] = { ds =>
    ds.map { car =>
      val yearsSincePurchase = calculateCarAge(car.date_of_purchase)

      CarsAge(
        car.id,
        car.price,
        car.brand,
        car.type_car,
        car.mileage,
        car.color,
        car.date_of_purchase,
        avgMileage,
        yearsSincePurchase
      )
    }
  }

  val carsPipeline: Dataset[CarsAge] = readCarsData("src/main/resources/cars.csv")
    .transform(normalizeColumnNames)
    .transform(unifyDateFormats)
    .transform(fillMissingMileage)
    .as[Cars]
    .transform { ds =>
      val avgMileage = calculateAverageMileage(ds)
      println(s"Average mileage: $avgMileage")
      addCarAgeInfo(avgMileage)(ds)
    }

  carsPipeline.show()
}