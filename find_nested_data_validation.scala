import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions.{concat, lit}

def createValidAndinvalidDf(conditionArray : Array[String], deequCsv : DataFrame, errorColumn: String) : DataFrame = {
  val applyValidFilter = conditionArray.mkString(" AND ")
  val applyInValidFilterArray = conditionArray.map(i => "!("+i+")")
  val applyInValidFilter = applyInValidFilterArray.mkString(" OR ")
  val validRecord = deequCsv.filter(applyValidFilter)
  print("validRecord.count ==> "+validRecord.count)
  var invalidRecord = deequCsv.filter(applyInValidFilter)
  invalidRecord.createOrReplaceTempView("invalidRecord")
  invalidRecord = spark.sql("select row_number() over (order by 1) as rnk,* from invalidRecord ")
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
  val invalidDf = invalidRow.toSeq.toDF("rnk", "headerError")
  
  invalidRecord = invalidRecord.join(invalidDf,Seq("rnk"),"inner").drop("rnk")
  
  //Join Valid and In valid reord in single df
  var dfArray = validRecord.withColumn(errorColumn,lit("")).union(invalidRecord).orderBy("EmpId")
  dfArray
}

val deequCsv = spark.read.json("/tmp/sample.json")
// Exploding the topping column using explode as it is an array type
// Extracting Code from col using DOT form

// convert nested data to dataframe
val nestedJson = deequCsv.select($"EmpID",explode($"key")).withColumn("Code",$"col.Code")
.withColumn("Id",$"col.Id").withColumn("Ntwk",$"col.Ntwk").drop($"col")

// find duplicate based on empid and key_id
val uniqueNestedData = nestedJson.groupBy("EmpId", "Id").count()

// create new dataframe from main nested df and duplicate df 
val orgNestedDt = uniqueNestedData.join(nestedJson, Seq("EmpId", "Id"), "inner")

val nestedConditionArray = Array(" length(Ntwk) = 6 ", "(CODE = ID OR (ID = 'TOR' AND CODE = 'EXP'))", " count = 1 ")

val nestedJsonDf = createValidAndinvalidDf(nestedConditionArray, orgNestedDt, "error")

// concate all error based on empid in nested array
val dfWithEmpIdAndError = nestedJsonDf.orderBy("EmpId").groupBy("EmpId").agg(array_join(collect_set("error"),delimiter=", ").alias("error"))

// only select header data
var headerData = deequCsv.select("code","ContractExpDate","EmpID","EmpName","EmpNo")
headerData = headerData.join(headerData.groupBy("EmpID").count(),Seq("EmpID"), "inner")

val headerConditionArray = Array(" count = 1 ")

val headerValidation = createValidAndinvalidDf(headerConditionArray, headerData, "headerError")

// concat main header error with nested all error
val headerCombinValidation = headerValidation.join(dfWithEmpIdAndError, Seq("EmpId"), "inner").select(col("EmpId"),concat(col("headerError"), lit(" "), col("error")).alias("error"))

val validAndInvalidData = deequCsv.join(headerCombinValidation, Seq("EmpId"), "inner").distinct()

val validRecord = validAndInvalidData.filter(" trim(error) = '' ")

val invalidRecord = validAndInvalidData.filter(" trim(error) <> '' ")
