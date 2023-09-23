package org.challenge.service

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{avg, col, substring}
import org.springframework.stereotype.Service
import org.challenge.repository.StockRepository
import org.challenge.utils.SparkSessionSingleton

import java.io.{BufferedWriter, File, FileWriter}

@Service
class StockService {
  var stockRepository: StockRepository = new StockRepository

  def stockStats(stock: String): String = {
    val sparkSession = SparkSessionSingleton.getSession

    val csvString = stockRepository.callAPI(stock).stripMargin
    val filePath = s"./src/main/resources/$stock.csv"

    createFile(filePath, csvString)

    val baseDataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    val dfWithHours: DataFrame = baseDataFrame
      .withColumn("hour", substring(col("timestamp"), 12, 2))
      .withColumn("avgPrice", (col("low") + col("high")) / 2)

    val resultDF: DataFrame = dfWithHours.groupBy("hour").agg(
      avg("low").alias("avgMin"),
      avg("high").alias("avgHigh"),
      avg("avgPrice").alias("avg")
    ).orderBy("hour")

    val result = s"$stock = ${resultDF.toJSON.collect().mkString(",")}\n"

    deleteFile(filePath)

    result
  }

  def createFile(filePath: String, csvString: String): Unit = {
    val fileWriter = new FileWriter(filePath)
    val bufferedWriter = new BufferedWriter(fileWriter)

    bufferedWriter.write(csvString)
    bufferedWriter.close()
    fileWriter.close()
  }

  def deleteFile(filePath: String): Unit = {
    val fileToDelete = new File(filePath)

    if (fileToDelete.exists()) {
      if (fileToDelete.delete()) {
        println(s"File $filePath deleted successfully.")
      } else {
        println(s"Failed to delete $filePath.")
      }
    } else {
      println(s"File $filePath does not exist.")
    }
  }

}
