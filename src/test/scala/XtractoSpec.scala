import Date_shape.dateShapeConstraint
import TagExtractor.{FileContent, callFunctionByName, extractComments_and_parsedCode_FromFile, processChecks}
import Unique.uniqueConstraint
import breeze.linalg.*
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.CheckStatus
import org.apache.spark.sql.catalyst.ScalaReflection.universe.Quasiquote
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.Source
import org.scalamock.MockFactoryBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalamock.scalatest.MockFactory

import scala.meta.XtensionQuasiquoteSource
import scala.meta.internal.fastparse.core.Parsed
class XtractoSpec  extends AnyWordSpec with Matchers with MockFactory {

  "callfunctionbyname" should {
    "return a DataFrame with correct properties" in {
      val result: Any = callFunctionByName("RandomQuality", "createDataFrame", Seq.empty)
      val dataFrameResult: DataFrame = result.asInstanceOf[DataFrame]
      dataFrameResult.columns.length shouldBe 3
      dataFrameResult.count() shouldBe 2
      val expectedColumnNames = Seq("Column1", "Column2", "Column3")
      dataFrameResult.columns should contain theSameElementsAs expectedColumnNames
    }
  }
  "uniquecall" should {
    "return a unique column success check" in {
      val result: Any = callFunctionByName("RandomQuality", "createDataFrame", Seq.empty)
      val dataFrameResult: DataFrame = result.asInstanceOf[DataFrame]
      val last_result: Any = callFunctionByName("Unique", "unique", Seq(dataFrameResult, "Column1"))
      val uniqueresult: VerificationResult = last_result.asInstanceOf[VerificationResult]
      uniqueresult.status shouldBe CheckStatus.Success
    }
  }
  "dateshapecall" should {
    "return a warning dateshape check" in {
      val result: Any = callFunctionByName("RandomQuality", "createDataFrame", Seq.empty)
      val dataFrameResult: DataFrame = result.asInstanceOf[DataFrame]
      val last_result: Any = callFunctionByName("Date_shape", "date_shape", Seq(dataFrameResult, "Column3","yyyy-MM-dd"))
      val dateshape_result: VerificationResult = last_result.asInstanceOf[VerificationResult]
      dateshape_result.status shouldBe CheckStatus.Warning
    }
  }
  "extractComments_and_parsedCode_FromFile" should {
      "correctly extract comments and parse code from a file" in {
        // Specify the path to your test Scala file
        val filePath = "/Users/khalid/IdeaProjects/untitled/src/main/scala/RandomQuality.scala"
        val result: FileContent = extractComments_and_parsedCode_FromFile(filePath)
        result.comments shouldBe a[IndexedSeq[_]]
        result.comments should not be empty
        // Check the type of the parsed code
        result.parsed.get shouldBe a[scala.meta.Source]
        result.parsed shouldBe a[scala.meta.Parsed.Success[_]]
    }
  }
  val spark: SparkSession = SparkSession.builder().appName("test").master("local[2]").getOrCreate()

  "processChecks" should {
    "correctly process checks and return results" in {
      // Create a sample DataFrame for testing
      val data = Seq((1, "khalid"), (2, "issam"))
      val columns = Seq("id", "name")
      val df = spark.createDataFrame(data).toDF(columns: _*)
      // Specify some sample checks
      val sampleChecks = Array("unique(Column1)")
      // Call the function
      val results: List[(String, Any)] = processChecks(df, sampleChecks)
      results should not be empty
      results.size shouldBe sampleChecks.length
      // Test for invalid input
      val invalidData = Seq(("value1", "NA"), ("value2", "NA"))
      val invalidDF = spark.createDataFrame(invalidData).toDF("Column1", "Column2")

      // Specify a check that will throw an exception with invalid input

      // Use intercept to check if an exception is thrown
    }
  }
}
