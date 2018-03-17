package org.apn.spark.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.RowFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object WordCount {
  val SPACE = " "

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WordCount").getOrCreate()
    val fileDF = spark.read.textFile("src/test/resources/wordcount.txt")

    val rddResult: RDD[(String, Int)] = fileDF.rdd
      .flatMap(_.split(SPACE))
      .map((_, 1))
      .reduceByKey(_ + _)

    rddResult.sortBy(_._2).take(5).foreach(println)

    // The schema is encoded in a string
    val schemaString = "word wc";
    // Generate the schema based on the string of schema
    val fields = Array(
      DataTypes.createStructField(schemaString.split(SPACE)(0), DataTypes.StringType, true),
      DataTypes.createStructField(schemaString.split(SPACE)(1), DataTypes.IntegerType, true))

    val schema = DataTypes.createStructType(fields)
    val rowRDD: RDD[Row] = rddResult.map(x => RowFactory.create(x._1.trim(), Int.box(x._2)))
    val wcDF = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    wcDF.createOrReplaceTempView("word_count");

    // Creates a temporary view using the DataFrame
    val grpSF = spark.sql("select * from word_count sort by wc limit 5")
    grpSF.show()
    
    fileDF.createOrReplaceTempView("file_dataset");
    var wcSqlDF = spark.sql("SELECT word, count(1) AS wc FROM (select explode(split(value, ' ')) as word from file_dataset) group by word sort by wc limit 5")
    wcSqlDF.show()
    
    spark.stop();
  }
}