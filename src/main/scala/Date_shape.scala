import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckLevel, CheckResult}
import com.amazon.deequ.constraints.{ConstrainableDataTypes, ConstraintResult}
import org.apache.commons.lang3.time.DateParser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

object Date_shape {
    def dateShapeConstraint(column: String, format: String): Check = {
      Check(CheckLevel.Warning, s"$column date shape check")
        .satisfies(s"date_format($column, '$format') IS NOT NULL", s"Date format check for $column")
    }

    def date_shape(df: DataFrame, column: String, format: String): VerificationResult = {
      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._
      val verificationSuite = VerificationSuite()
        .onData(df)
        .addCheck(dateShapeConstraint(column, format))
        .run()
      verificationSuite
    }
}
