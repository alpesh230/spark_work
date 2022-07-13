// Databricks notebook source
import org.apache.spark.sql.DataFrame
/*
  deequ.csv -> File Data Sample
  """
  EmpName,ContractExpDate,EmpNo,ID,Code
  Test1,2022-07-12,123456889,ABC,ABC
  Test2,2022-07-13,123456,ABC,ABC
  Test3,2022-07-14,1234568,ABC,ABC
  Test4,2022-07-15,12345687,ABC,ABC
  Test5,2022-07-16,123456889,ABC,ABC
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
// informSchema = true means its take automatically schema 
val deequCsv = spark.read.option("header", "true").option("inferSchema","true").csv("/tmp/deequ.csv")

val conditionArray = Array("EmpName IS NOT NULL", "to_date(ContractExpDate) >= CURRENT_DATE", "length(EmpNo) BETWEEN 6 AND 8", "length(ID) = 3 AND ID = Code ")

def createValidAndinvalidDf(conditionArray : Array[String], deequCsv : DataFrame) : Array[DataFrame] = {
 //here merge all business rule with and
  val applyValidFilter = conditionArray.mkString(" AND ")

  val applyInValidFilterArray = conditionArray.map(i => "!("+i+")")
  // here merge all business rule with or
  val applyInValidFilter = applyInValidFilterArray.mkString(" OR ")
  val validRecord = deequCsv.filter(applyValidFilter)
  print("validRecord.count ==> "+validRecord.count)
  var invalidRecord = deequCsv.filter(applyInValidFilter)
  invalidRecord.createOrReplaceTempView("invalidRecord")
  invalidRecord = spark.sql("select row_number() over (order by empname) as rnk,* from invalidRecord ")
  print("invalidRecord.count ==> "+invalidRecord.count)
  var invalidRow = scala.collection.mutable.Map[String, String]()
  for (i <- applyInValidFilterArray){
    val newDf = invalidRecord.filter(i).select("rnk")
    for (j <- newDf.rdd.collect){
      var value = i;
      if (invalidRow.contains(s"${j(0)}")){
         value = invalidRow.get(s"${j(0)}") +","+ value
      } 
      invalidRow(s"${j(0)}") = value
    }
  }
  val invalidDf = invalidRow.toSeq.toDF("rnk", "error")
  
  invalidRecord = invalidRecord.join(invalidDf,Seq("rnk"),"inner").select("empname", "contractexpdate", "empno", "id", "code", "error")
  
  var dfArray = Array(validRecord,invalidRecord)
  dfArray
}

var dfArray = createValidAndinvalidDf(conditionArray, deequCsv)
var validRecord = dfArray(0)
var inValidRecorf = dfArray(1)

validRecord.write.option("header",true).csv("/tmp/valid4.csv")
inValidRecorf.write.option("header",true).csv("/tmp/invalid4.csv")

// COMMAND ----------


