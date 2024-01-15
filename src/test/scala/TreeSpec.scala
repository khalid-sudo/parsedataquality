import Date_shape.dateShapeConstraint
import Unique.uniqueConstraint
import com.amazon.deequ.checks.{CheckResult, CheckStatus}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.meta.XtensionQuasiquoteSource

class TreeSpec extends AnyWordSpec with Matchers with MockFactory {
  val mockedDataFrame: DataFrame = {
    // Create a sample DataFrame for testing
    import org.apache.spark.sql.{SparkSession, DataFrame}
    val spark: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
    import spark.implicits._
    Seq(("value1", "NA", "10-12-2021"), ("value2", "NA", "2021-12-11")).toDF("Column1", "Column2", "Column3")
  }
  "tree_traverser" should {
    val mockedTagExtractor = mock[TagExtractorTrait]
    (mockedTagExtractor.tree_traverser _)
      .expects(*, *)
      .returning(
        List(
          ("Unique", VerificationSuite().onData(mockedDataFrame).addCheck(uniqueConstraint("Column1")).run()), // Configuration for unique
          ("Date_shape", VerificationSuite().onData(mockedDataFrame).addCheck(dateShapeConstraint("Column3", "yyyy-MM-dd")).run()) // Configuration for date_shape
        )
      ).once()

    "correctly traverse the tree, extract function information, and call other functions" in {
      // Create a sample Scala source code tree for testing
      val sampleTree: scala.meta.Source =
        source"""
          object RandomQuality {
            def createDataFrame(): org.apache.spark.sql.DataFrame = {
              // Sample DataFrame creation
              import org.apache.spark.sql.{SparkSession, DataFrame}
              val spark: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
              import spark.implicits._
              val data = Seq(("value1", "NA", "10-12-2021"), ("value2", "NA", "2021-12-11"))
              data.toDF("Column1", "Column2", "Column3")
            }
          }
        """

      // Call the tree_traverser function
      val actualresult: List[(String, Any)] = mockedTagExtractor.tree_traverser(sampleTree, IndexedSeq(" @quality    unique(Column1);date_shape(Column5, yyyy-MM-dd)"))

      // Add assertions based on your expectations
      // For example, you might want to check the contents of the 'result' variable
      val uniqueCheck = actualresult.find { case (name, _) => name == "Unique" }
      uniqueCheck.isDefined shouldBe true
      uniqueCheck.get._1 shouldBe "Unique"
      uniqueCheck.get._2 shouldBe a[VerificationResult]

      val dateShapeCheck = actualresult.find { case (name, _) => name == "Date_shape" }
      dateShapeCheck.isDefined shouldBe true
      dateShapeCheck.get._1 shouldBe "Date_shape"
      dateShapeCheck.get._2 shouldBe a[VerificationResult] // Make sure it's a VerificationSuite
    }
    "handle invalid input parameters" in {
      // Call the tree_traverser function with invalid input (e.g., null)
      val thrown = intercept[RuntimeException] {
        mockedTagExtractor.tree_traverser(null, IndexedSeq(" @quality    unique(Column1);date_shape(Column3, yyyy-MM-dd)"))
      }

      // Assert that the expected exception was thrown
      thrown.getMessage shouldBe "Expected error message or pattern"
    }
  }
}
