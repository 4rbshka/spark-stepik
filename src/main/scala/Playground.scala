import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Playground extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  private def createSession(appName: String) = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }

  val appName: String = "Playground"
  lazy val spark = createSession(appName)

  def processSeason(filePath: String, wordCol: String, countCol: String) = {
    spark.read
      .option("inferSchema", "true")
      .csv(filePath)
      .select(explode(split(lower(col("_c0")), "\\W+")).as(wordCol))
      .filter(length(col(wordCol)) > 0)
      .groupBy(col(wordCol))
      .agg(count(col(wordCol)).as(countCol))
      .orderBy(desc(countCol))
      .limit(20)
      .withColumn("id", monotonically_increasing_id())
  }

  val season1DF = processSeason("src/main/resources/subtitles_s1.json", "w_s1", "cnt_s1")
  val season2DF = processSeason("src/main/resources/subtitles_s2.json", "w_s2", "cnt_s2")

  val wordCountDF = season1DF
    .join(season2DF, "id")
    .select("w_s1", "cnt_s1", "id", "w_s2", "cnt_s2")

  wordCountDF.write
    .mode(SaveMode.Overwrite)
    .json("src/main/resources/data/wordcount")

  spark.stop()
}