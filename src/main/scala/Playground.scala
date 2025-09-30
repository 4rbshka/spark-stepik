import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Playground extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Playground")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  val data = Seq(
    Row(
      "s9FH4rDMvds",
      "2020-08-11T22:21:49Z",
      "UCGfBwrCoi9ZJjKiUK8MmJNw",
      "2020-08-12T00:00:00Z"),
    Row(
      "kZxn-0uoqV8",
      "2020-08-11T14:00:21Z",
      "UCGFNp4Pialo9wjT9Bo8wECA",
      "2020-08-12T00:00:00Z"),
    Row(
      "QHpU9xLX3nU",
      "2020-08-10T16:32:12Z",
      "UCAuvouPCYSOufWtv8qbe6wA",
      "2020-08-12T00:00:00Z")
  )

  val schema = Array(
    StructField("videoId", StringType, false),
    StructField("publishedAt", StringType, false),
    StructField("channelId", StringType, false),
    StructField("trendingDate", StringType, false)
  )


  val df = spark.createDataFrame(
    sc.parallelize(data),
    StructType(schema)
  )

  df.show()
  df.printSchema()

}