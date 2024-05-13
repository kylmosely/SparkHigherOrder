package db

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object DBConnection {
  private val dbUrl = "jdbc:db2://localhost:50000/testdb"
  val dbTable = "test.context_conc"
  val connectionProperties = new java.util.Properties()
  connectionProperties.put("user", "db2inst1")
  connectionProperties.put("password", "password")

  def readFromDB(spark: SparkSession): DataFrame = {
    spark.read.jdbc(dbUrl, dbTable, connectionProperties)
  }
}
