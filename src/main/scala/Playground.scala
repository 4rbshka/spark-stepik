import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

object Playground extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  override val appName: String = "Playground"

  val codefixesDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/code_fixes.csv")

  val resultDF = codefixesDF
    .select(
      when(col("syntax corrections") === 1, "syntax corrections")
        .when(col("improved readability") === 1, "improved readability")
        .when(col("updated function definition") === 1, "updated function definition")
        .when(col("fixed bug") === 1, "fixed bug")
        .otherwise("no_fix")
        .as("fix_description"),
      col("code_lines")
    )
    .filter(col("fix_description") =!= "no_fix")
    .groupBy("fix_description")
    .agg(
      count("*").as("fix_count"),
      sum("code_lines").as("lines_count")
    )
    .withColumn("total_count",
      struct(
        sum("fix_count").over().as("total_fixes"),
        sum("lines_count").over().as("total_lines")
      )
    )

  resultDF.show(truncate = false)
}