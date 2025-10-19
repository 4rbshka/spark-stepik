import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Dataset

object Playground extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  override val appName: String = "Playground"
  import spark.implicits._

  case class Employee(
                       positionID: Int,
                       position: String
                     )

  val hrDataset: Dataset[Employee] = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/hrdataset.csv")
    .as[Employee]
    .map(employee => Employee(employee.positionID, employee.position))

  def extractFirstWord(position: String): String =
    position.split(" ").headOption.getOrElse("").toLowerCase

  def findPositionsByPartialNames(partialNames: List[String]): Dataset[Employee] = {
    val searchWords = partialNames.map(_.toLowerCase).toSet

    hrDataset
      .filter(employee => searchWords(extractFirstWord(employee.position)))
      .distinct()
      .orderBy("positionID")
  }

  val query = List("BI", "it")
  val resultDS = findPositionsByPartialNames(query)

  resultDS.show(false)
}