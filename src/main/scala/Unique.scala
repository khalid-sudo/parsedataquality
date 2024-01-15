import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.apache.spark.sql.DataFrame

object Unique {
  def uniqueConstraint(column: String): Check = {
    Check(CheckLevel.Warning,column + " uniqueness check")
      .hasCompleteness(column,_ >= 0.95)
      .isUnique(column)

  }
  def unique(df: DataFrame, column: String): VerificationResult = {
    val verificationSuite = VerificationSuite()
      .onData(df)
      .addCheck(uniqueConstraint(column))
      .run()
    verificationSuite
  }
}