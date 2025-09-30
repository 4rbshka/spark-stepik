import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Playground extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Playground")
    .master("local")
    .getOrCreate()

  val restaurantexSchema = StructType(Seq(
    StructField("average_cost_for_two", LongType),
    StructField("cuisines", StringType),
    StructField("deeplink", StringType),
    StructField("has_online_delivery", IntegerType),
    StructField("is_delivering_now", IntegerType),
    StructField("menu_url", StringType),
    StructField("name", StringType),
    StructField("user_rating",
      StructType(Seq(
        StructField("aggregate_rating", StringType),
        StructField("rating_color", StringType),
        StructField("rating_text", StringType),
        StructField("votes", StringType)
      )))
  ))

  val restaurantexDF = spark.read
    .format("json")
    .schema(restaurantexSchema)
    .load("src/main/resources/restaurant_ex.json")

  val selectedColumns = restaurantexDF.select("has_online_delivery", "is_delivering_now")

  selectedColumns.printSchema()
}