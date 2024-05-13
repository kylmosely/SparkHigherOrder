package transformer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object TransformFunc {
  def transformA(df: DataFrame, context: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.withColumn("transformed", when(udfTransform($"bucket", lit(context)) && $"value" < 10 && $"transformed".isNull, lit("Applied A")).otherwise($"transformed"))
  }

  def transformB(df: DataFrame, context: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.withColumn("transformed", when(udfTransform($"bucket", lit(context)) && $"transformed".isNull, lit("Applied B")).otherwise($"transformed"))
  }

  private val nonMissing: (String, String) => Boolean = (bucket, context) => {
    bucket != "missing info"
  }

  private val udfTransform = udf(nonMissing)
}
