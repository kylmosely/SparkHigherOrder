import db.DBConnection
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import transformer.TransformFunc._
import utils.SparkSessionBuilder

object Script extends App {
  implicit val spark: SparkSession = SparkSessionBuilder.buildSession("db2")
  import spark.implicits._

  private val dfInit = DBConnection.readFromDB(spark)
  val df = dfInit.toDF(dfInit.columns.map(_.toLowerCase): _*)
  val initialDF = if (!df.columns.contains("transformed")) df.withColumn("transformed", lit(null).cast("string")) else df

  private val bucketContextTransformations: Map[String, Map[String, Seq[(DataFrame, String) => DataFrame]]] = Map(
    "Lost" -> Map(
      "context1" -> Seq(transformA, transformB),
      "context2" -> Seq(transformB)),
    "missort" -> Map(
      "context1" -> Seq(transformB),
      "context2" -> Seq(transformA)
    )
  )

  private val ProcessedBuckets: Seq[DataFrame] = bucketContextTransformations.flatMap { case (bucket, contextMap) =>
    contextMap.map { case (context, transformations) =>
      val filteredDF = initialDF.filter($"bucket" === bucket && $"context" === context)
      val finalDF = transformations.foldLeft(filteredDF)((currentDF, transformation) => transformation(currentDF, context))
      finalDF
    }
  }.toSeq

  private val unifiedDataFrame: DataFrame = ProcessedBuckets.reduce(_ union _)
  unifiedDataFrame.repartition(200).show()
}
