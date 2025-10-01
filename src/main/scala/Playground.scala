import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._

object Playground extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  override val appName: String = "Playground"

  val moviesSchema = StructType(Seq(
    StructField("index", IntegerType),
    StructField("show_id", StringType),
    StructField("type", StringType),
    StructField("title", StringType),
    StructField("director", StringType),
    StructField("cast", StringType),
    StructField("country", StringType),
    StructField("date_added", StringType),
    StructField("release_year", IntegerType),
    StructField("rating", StringType),
    StructField("duration", StringType),
    StructField("listed_in", StringType),
    StructField("description", StringType),
    StructField("year_added", IntegerType),
    StructField("month_added", FloatType),
    StructField("season_count", IntegerType)
  ))

  val moviesDF = spark.read
    .format("csv")
    .schema(moviesSchema)
    .option("header", "true")
    .option("mode", "failFast")
    .option("nullValue", "n/a")
    .option("escape", "\"")
    .option("quote", "\"")
    .option("multiLine", "true")
    .load("src/main/resources/movies_on_netflix.csv")

  moviesDF.printSchema()

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies_on_netflix.parquet")
}