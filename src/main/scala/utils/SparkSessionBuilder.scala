package utils

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {
  def buildSession(appName: String): SparkSession = {
    SparkSession.builder
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }
}
