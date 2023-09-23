package org.challenge.repository

import org.challenge.utils.SparkSessionSingleton
import org.springframework.context.annotation.Bean


class StockRepository {
  def callAPI(stock: String): String = {
    val apiKey = "89O9YCG82J5M8LLS"
    val interval = "5min"
    val sparkSession = SparkSessionSingleton.getSession

    val apiUrl = s"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=$stock&interval=$interval&apikey=$apiKey&datatype=csv"

    scala.io.Source.fromURL(apiUrl).mkString
  }
}
