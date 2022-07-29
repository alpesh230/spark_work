// Databricks notebook source
// Reading json file and create dataframe
val invalidDf = spark.read.json("/tmp/sample2.json")
val validDf = spark.read.json("/tmp/sample.json")

// COMMAND ----------

import org.apache.spark.sql.types._

// Create default right schema so i can compare all dataframe with this schema only
val defaultSchema = new StructType().add("Code",LongType,true) 
      .add("ContractExpDate",StringType,true) 
      .add("EmpID",LongType,true)
      .add("EmpName",StringType,true)
      .add("EmpNo",LongType,true) 
      .add("key", ArrayType(new StructType().add("Code",StringType,true).add("Id",StringType,true).add("Ntwk",LongType,true),true),true)

// This method convert schema to Seq[String] in this Sequence we have column or fields name and data type like this "EmpId|LongType"
def flattenSchema(schema: StructType): Seq[String] = {
  schema.fields.flatMap {
    case StructField(name, inner: StructType, _, _) => Seq(s"${name}|StructType") ++ flattenSchema(inner)
    case StructField(name, ArrayType(structType: StructType, _), _, _) => Seq(s"${name}|ArrayType") ++flattenSchema(structType)
    case StructField(name, types, _, _) => Seq(s"${name}|${types}")
  }
}

// In This Method we compare one by one fields and datatypes
def compareSchema(defaultSchema : StructType, dataSchema : StructType): Boolean = {
    val defaultSchemaSeq = flattenSchema(defaultSchema)
    val dfSchemaSeq = flattenSchema(dataSchema)
  
    // here we directly check both schema is valid or not
    if (defaultSchema != dataSchema) {
        if (defaultSchemaSeq.size == dfSchemaSeq.size) {
            for (i <- 0 to defaultSchemaSeq.size - 1) {
               var defaultStr = defaultSchemaSeq(i)
               var dfStr = dfSchemaSeq(i)
               var defaultStrSplit = defaultStr.split("\\|")
               var dfStrSplit = dfStr.split("\\|")
               var defaultStrName = defaultStrSplit(0)
               var dfStrName = dfStrSplit(0)
               var defaultStrType = defaultStrSplit(1)
               var dfStrType = dfStrSplit(1)
               if (defaultStrName != dfStrName) {
                  println(s"Name not matching ${defaultStrName} ${dfStrName}")
                  return false
               }

               if (defaultStrType != dfStrType) {
                  println(s"Data Type not matching ${defaultStrName} ${dfStrName}")
                  return false
               }
            }
            return true
        } else {
            println("Column size is not match...!")
            return false
        }
    } else {
        println("Schema valid")
        return true
    }
}

// COMMAND ----------

// Find which method is invalid and why
compareSchema(defaultSchema, invalidDf.schema)

// COMMAND ----------

compareSchema(defaultSchema, validDf.schema)

// COMMAND ----------


