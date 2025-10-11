import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Playground extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  private def createSession(appName: String) =
    SparkSession.builder().appName(appName).master("local[*]").getOrCreate()

  val appName: String = "Playground"
  lazy val spark = createSession(appName)

  import spark.implicits._

  case class CleanJob(jobTitle: String, company: String, location: String, reviewsCount: Int)

  case class LeaderStats(name: String, statusType: String, location: String, count: Long, countType: String)

  def analyzeWithDataFrame(filePath: String): DataFrame = {
    val cleanedDF = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("multiLine", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(filePath)
      .na.drop()
      .withColumn("Company", regexp_replace(trim(col("Company")), "\n", ""))
      .withColumn("Company", regexp_replace(col("Company"), "\\s+", " "))
      .withColumn("Company", trim(col("Company")))
      .withColumn("JobTitle", trim(col("JobTitle")))
      .withColumn("Location", trim(col("Location")))
      .withColumn("ReviewsCount",
        regexp_extract(regexp_replace(col("CompanyReviews"), ",", ""), "(\\d+)", 1).cast("int")
      )
      .filter(col("ReviewsCount") > 0)

    def findLeadersDF(df: DataFrame, groupByCol: String, statusType: String): DataFrame = {
      val totals = df.groupBy(groupByCol).agg(sum("ReviewsCount").as("total_reviews"))
      val maxReviews = totals.agg(max("total_reviews")).as[Long].first()
      val leaders = totals.filter(col("total_reviews") === maxReviews)

      val leaderStats = df.join(leaders, groupByCol)
        .groupBy(groupByCol, "Location")
        .agg(sum("ReviewsCount").as("location_reviews"))
        .groupBy(groupByCol)
        .agg(
          min(struct(col("location_reviews"), col("Location"))).as("min_loc"),
          max(struct(col("location_reviews"), col("Location"))).as("max_loc")
        )

      val minRows = leaderStats.select(
        col(groupByCol).as("name"),
        lit(statusType).as("status_type"),
        col("min_loc.Location").as("location"),
        col("min_loc.location_reviews").as("count"),
        lit("min").as("count_type")
      )

      val maxRows = leaderStats.select(
        col(groupByCol).as("name"),
        lit(statusType).as("status_type"),
        col("max_loc.Location").as("location"),
        col("max_loc.location_reviews").as("count"),
        lit("max").as("count_type")
      )

      minRows.union(maxRows)
    }

    findLeadersDF(cleanedDF, "Company", "company")
      .union(findLeadersDF(cleanedDF, "JobTitle", "job"))
  }

  def analyzeWithDataset(filePath: String): Dataset[LeaderStats] = {
    val cleanJobsDF = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("multiLine", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(filePath)
      .na.drop()
      .withColumn("Company", regexp_replace(trim(col("Company")), "\n", ""))
      .withColumn("Company", regexp_replace(col("Company"), "\\s+", " "))
      .withColumn("Company", trim(col("Company")))
      .withColumn("JobTitle", trim(col("JobTitle")))
      .withColumn("Location", trim(col("Location")))
      .withColumn("ReviewsCount",
        regexp_extract(regexp_replace(col("CompanyReviews"), ",", ""), "(\\d+)", 1).cast("int")
      )
      .filter(col("ReviewsCount") > 0)
      .select(
        col("JobTitle").as("jobTitle"),
        col("Company").as("company"),
        col("Location").as("location"),
        col("ReviewsCount").as("reviewsCount")
      )

    val cleanJobsDS = cleanJobsDF.as[CleanJob]

    def findLeadersDS(ds: Dataset[CleanJob], groupByCol: String, statusType: String): Dataset[LeaderStats] = {
      val leaderData = ds.toDF()
        .groupBy(groupByCol)
        .agg(sum("reviewsCount").as("totalReviews"))
        .withColumn("maxReviews", max("totalReviews").over())
        .filter(col("totalReviews") === col("maxReviews"))
        .drop("maxReviews")
        .join(ds.toDF(), groupByCol)
        .groupBy(groupByCol, "location")
        .agg(sum("reviewsCount").as("locationReviews"))
        .groupBy(groupByCol)
        .agg(
          min(struct(col("locationReviews"), col("location"))).as("minLoc"),
          max(struct(col("locationReviews"), col("location"))).as("maxLoc")
        )

      val minRows = leaderData
        .select(
          col(groupByCol).as("name"),
          lit(statusType).as("statusType"),
          col("minLoc.location").as("location"),
          col("minLoc.locationReviews").as("count"),
          lit("min").as("countType")
        )
        .as[LeaderStats]

      val maxRows = leaderData
        .select(
          col(groupByCol).as("name"),
          lit(statusType).as("statusType"),
          col("maxLoc.location").as("location"),
          col("maxLoc.locationReviews").as("count"),
          lit("max").as("countType")
        )
        .as[LeaderStats]

      minRows.union(maxRows)
    }

    val companyLeaders = findLeadersDS(cleanJobsDS, "company", "company")
    val jobLeaders = findLeadersDS(cleanJobsDS, "jobTitle", "job")

    companyLeaders.union(jobLeaders)
  }

  val resultDF = analyzeWithDataFrame("src/main/resources/AiJobsIndustry.csv")
  resultDF.show(false)

  val resultDS = analyzeWithDataset("src/main/resources/AiJobsIndustry.csv")
  resultDS.show(false)
}