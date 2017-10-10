import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object SqlSchemaSepc {
  val schema = StructType(Array(
    StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("segments", ArrayType(IntegerType))
  ))

  def segSum(s: Seq[Int]): Int= {
    var c:Int = 0
    s.foreach(c += _)
    c
  }

  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("sqlSchemaSepc")
      .getOrCreate()

    spark.sqlContext.udf.register("ssum", segSum _)
    import spark.implicits._

    val distData = spark
      .sparkContext
      .textFile("/home/shunj/xad/study/resources/sql/spsegments.txt")
      .map(_.split(" "))
      .map(atr=>Row(atr(0), atr(1).toInt, atr(2).split(",").map(_.trim.toInt)))

    val df = spark.createDataFrame(distData, schema)

    df.createOrReplaceTempView("segment")

    df.show()

    spark.sql("select name, age, ssum(segments) from segment where age > 10").show()



  }

}
