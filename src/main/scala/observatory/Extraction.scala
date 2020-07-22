package observatory

import java.time.LocalDate

import scala.io.Source
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession, types}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {
  val spark: SparkSession =
    SparkSession.builder().appName("Observatory").config("spark.master", "local").getOrCreate()
  import spark.implicits._

  val STN_id = StructField("STN_id",DataTypes.StringType)
  val WBAN_id = StructField("WBAN_id", DataTypes.StringType)
  val latitude = StructField("latitude", DataTypes.DoubleType)
  val longitude = StructField("longitude", DataTypes.DoubleType)
  val month = StructField("month", DataTypes.IntegerType)
  val day = StructField("day", DataTypes.IntegerType)
  val temperature = StructField("temperature", DataTypes.DoubleType)

  val stationsschema = StructType(Array(STN_id, WBAN_id, latitude, longitude))
  val temperatureschema = StructType(Array(STN_id, WBAN_id, month, day, temperature))
  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {

//    val stationsResourcePath = getClass.getResource(stationsFile).getPath
//    val temperaturesResourcePath = getClass.getResource(temperaturesFile).getPath
    val stationsResourcePath = filePath(stationsFile)

    val temperaturesResourcePath = filePath(temperaturesFile)

    val stations = spark.read
      .schema(stationsschema)
      .option("header", value = false)
      .csv(stationsResourcePath)
    val temperatures = spark.read
      .schema(temperatureschema)
      .option("header", value = false)
      .csv(temperaturesResourcePath)

    val CorrectStations = stations.filter("latitude IS NOT NULL and longitude IS NOT NULL")

//    val CorrectStations = stations.filter(stations("latitude") !== 0)
    val joined = CorrectStations.join(temperatures, stations("WBAN_id") 
      <=> temperatures("WBAN_id") && stations("STN_id") <=> temperatures("STN_id"))

    joined.rdd.map(row => {
      val temperature = (row.getAs[Double]("temperature") - 32) * 5 / 9
      val location = Location(row.getAs[Double]("latitude"), row.getAs[Double]("longitude"))
      val localDate = LocalDate.of(year, row.getAs[Int]("month"), row.getAs[Int]("day"))
      (localDate, location, temperature)
    }).collect
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    val recordsRDD = spark.sparkContext.parallelize(records.toSeq).map { case (_, location, temp) => (location, temp) }
    val recordsDS = recordsRDD.toDS()
      .withColumnRenamed("_1", "location")
      .withColumnRenamed("_2", "temperature")

    recordsDS.groupBy("location").mean("temperature").as[(Location, Double)].collect
  }

  private def filePath(fileName: String): Dataset[String] = {
    val fileStream = Source.getClass.getResourceAsStream(fileName)
    spark.sparkContext.makeRDD(Source.fromInputStream(fileStream).getLines().toList).toDS
  }
}
