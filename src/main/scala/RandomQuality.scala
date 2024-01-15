import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
object RandomQuality {
  /**
   * Main Run Execution for the function main
   *
   * @database [[my_database_db]]
   * @table [[my_table_db]]
   * @return [[dictionnaryStr_dataproduct_mainRun]]
   * @quality       unique(Column1);date_shape(Column3, yyyy-MM-dd)
   */
  def createDataFrame(): org.apache.spark.sql.DataFrame = {
    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("akka").setLevel(Level.INFO)
    val spark: SparkSession = SparkSession.builder()
      .appName("createDataFrame")
      .master("local[*]") // you can adjust this as needed
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val dataSeq = Seq(("value1", "NA", "10-12-2021"), ("value2", "NA", "2021-12-11"))
    val df = dataSeq.toDF("Column1", "Column2", "Column3")
    //val df: org.apache.spark.sql.DataFrame = data.toDF("Name", "Age")
    df
  }

}
