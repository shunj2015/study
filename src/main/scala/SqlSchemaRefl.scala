import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SqlSchemaRefl {
  case class SegmentPair(name:String, segs:Array[Long])
  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sqlschemrfl").getOrCreate()
    import spark.implicits._

    val df = spark.sparkContext
      .textFile("/home/shunj/xad/study/resources/sql/segments.txt")
      .map(_.split(" ")).map(atr=>SegmentPair(atr(0), atr(1).split(",").map(_.trim.toInt))).toDF()

    df.createOrReplaceTempView("segment")

    spark.sql("select * from segment").show()
  }

}
