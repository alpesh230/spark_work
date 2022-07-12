// Databricks notebook source
/*
  deequ.csv -> File Data Sample
  """
  EmpName,ContractExpDate,EmpNo,ID,Code
  Test1,2022-07-12,123456,ABC,ABC
  Test2,2022-07-13,123456,ABC,ABC
  Test3,2022-07-14,1234568,ABC,ABC
  Test4,2022-07-15,12345687,ABC,ABC
  Test5,2022-07-16,12345688,ABC,ABC
  Test6,2022-07-17,1234568,ABC,ABC
  Test7,2022-07-18,1234568,ABC,ABC
  Test8,2022-07-19,1234568,ABC,ABC
  Test9,2022-07-20,1234568,ABC,ABC
  Test10,2022-07-21,1234568,ABC,ABC
  Test11,2022-07-22,1234568,ABC,ABC
  Test12,2022-07-23,1234568,ABC,ABC
  Test13,2022-07-24,1234568,ABC,ABC
  Test14,2022-07-25,1234568,ABC,ABC
  Test15,2022-07-26,1234568,ABC,ABC
  Test16,2022-07-27,1234568,ABC,ABC
  Test17,2022-07-28,1234568,ABC,ABC
  Test18,2022-07-29,1234568,ABC,ABCE
  Test19,2022-07-30,1234568,ABCD,ABC
  ,2022-07-31,1234568,ABC,ABC
  """
*/

val deequCsv = spark.read.option("header", "true").option("inferSchema","true").csv("/tmp/deequ.csv")

import scala.util.matching.Regex
import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.VerificationResult.successMetricsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.constraints.ConstraintStatus

val _check = Check(CheckLevel.Error, "Data Validation Check")
  .satisfies(
            "EmpName IS NOT NULL",
            "EmpName is Null"
        )
  .satisfies(
            "ContractExpDate >= current_date()",
            "Invalid ContractExpDate"
        )
  .satisfies(
            "length(EmpNo) BETWEEN 6 AND 8",
            "EmpNo length must between 6 and 8"
        )
  .satisfies(
            "length(ID) = 3 AND ID = Code",
            "ID length should be 3. ID and Code always same"
        )

val verificationResult: VerificationResult = { VerificationSuite()
  .onData(deequCsv)
  .addCheck(_check)
  .run()
}

val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)

// resultDataFrame.show(truncate=false)

val resultSuccessMetricsAsDF = successMetricsAsDataFrame(spark, verificationResult)

// resultSuccessMetricsAsDF.show(truncate=false)

if (verificationResult.status == CheckStatus.Success) {
  println("The data passed the test, everything is fine!")
} else {
  println("We found errors in the data:\n")

  val resultsForAllConstraints = verificationResult.checkResults
    .flatMap { case (_, checkResult) => checkResult.constraintResults }
  
  resultsForAllConstraints
    .filter { _.status != ConstraintStatus.Success } //<<This filters failed
    .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
}

// COMMAND ----------


