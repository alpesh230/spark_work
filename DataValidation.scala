import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object DataValidation {

  def createValidAndinvalidDf(conditionArray: Array[String], deequCsv: DataFrame, spark: SQLContext): Array[DataFrame] = {
    //here merge all business rule with and
    val applyValidFilter = conditionArray.mkString(" AND ")

    val applyInValidFilterArray = conditionArray.map(i => "!(" + i + ")")
    // here merge all business rule with or
    val applyInValidFilter = applyInValidFilterArray.mkString(" OR ")
    val validRecord = deequCsv.filter(applyValidFilter)
    println("validRecord.count ==> " + validRecord.count)
    var invalidRecord = deequCsv.filter(applyInValidFilter)
    // invalidRecord.createOrReplaceTempView("invalidRecord")
    // here add row_number bcs uniqueness of duplicate invalid rows
    val windowSpec = Window.partitionBy("ContractExpDate").orderBy("empname")
    invalidRecord = invalidRecord.withColumn("rnk",row_number.over(windowSpec))
	// This "WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation." 
	// issue is bcs of below line of code changed with line # 21 code
    // invalidRecord = spark.sql("select row_number() over (order by empname) as rnk,* from invalidRecord ")
    println("invalidRecord.count ==> " + invalidRecord.count)
    var invalidRow = scala.collection.mutable.Map[String, String]()
    for (i <- applyInValidFilterArray) {
      val newDf = invalidRecord.filter(i).select("rnk")
      for (j <- newDf.rdd.collect) {
        var value = i;
        if (invalidRow.contains(s"${j(0)}")) {
          value = invalidRow.get(s"${j(0)}") + "," + value
        }
        invalidRow(s"${j(0)}") = value
      }
    }

    import spark.implicits._
    val invalidDf = invalidRow.toSeq.toDF("rnk", "error")

    invalidRecord = invalidRecord.join(invalidDf, Seq("rnk"), "inner").select("empname", "contractexpdate", "empno", "id", "code", "error")

    var dfArray = Array(validRecord, invalidRecord)
    dfArray
  }

  def main(args: Array[String]): Unit = {
    println("Hello world!")
    val conf = new SparkConf().setAppName("LearnSpark").setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    val sq = ss.sqlContext
    val csv = sq.read.option("header", "true").option("inferSchema", "true")
      .csv("D:\\Project Data\\SparkDataFiles\\sample.csv")
    println(csv.count())
    println("Start Calling")
    val conditionArray = Array("EmpName IS NOT NULL", "to_date(ContractExpDate) >= CURRENT_DATE", "length(EmpNo) BETWEEN 6 AND 8", "length(ID) = 3 AND ID = Code ")

    var dfArray = createValidAndinvalidDf(conditionArray, csv, sq)
    var validRecord = dfArray(0)
    var invalidRecord = dfArray(1)
    println(s"validRecord Count = ${validRecord}")
    println(s"invalidRecord Count = ${invalidRecord}")
    validRecord.write.option("header", true).csv("D:\\Project Data\\SparkDataFiles\\valid.csv")
    invalidRecord.write.option("header", true).csv("D:\\Project Data\\SparkDataFiles\\invalid.csv")
  }
}
