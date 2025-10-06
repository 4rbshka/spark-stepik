import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Playground extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  override val appName: String = "Playground"
  import spark.implicits._

  case class Shoes(
                    item_category: String,
                    item_name: String,
                    item_after_discount: String,
                    item_price: String,
                    percentage_solds: Int,
                    item_rating: Int,
                    item_shipping: String,
                    buyer_gender: String
                  )

  def readData(filePath: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
  }

  def filterRequiredFields(df: DataFrame): DataFrame = {
    df.filter(col("item_name").isNotNull && col("item_category").isNotNull)
  }

  def fillItemAfterDiscount(df: DataFrame): DataFrame = {
    df.withColumn("item_after_discount",
      when(col("item_after_discount").isNull, col("item_price"))
        .otherwise(col("item_after_discount"))
    )
  }

  def fillNumericFields(df: DataFrame): DataFrame = {
    df.na.fill(Map(
      "item_rating" -> 0,
      "percentage_solds" -> -1
    ))
  }

  def fillStringFields(df: DataFrame): DataFrame = {
    df.na.fill(Map(
      "buyer_gender" -> "unknown",
      "item_price" -> "n/a",
      "item_shipping" -> "n/a"
    ))
  }

  val athleticShoesDS = readData("src/main/resources/athletic_shoes.csv")
    .transform(filterRequiredFields)
    .transform(fillItemAfterDiscount)
    .transform(fillNumericFields)
    .transform(fillStringFields)
    .as[Shoes]

  athleticShoesDS.show()
}