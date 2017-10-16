import org.apache.spark.sql.SparkSession

object SqlAdvance {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("advance sql")
      .getOrCreate()
    val userDF = spark.read.load("/usr/local/spark/examples/src/main/resources/users.parquet")
    userDF.select("favorite_color", "name").write.partitionBy("favorite_color").format("parquet").save("111.parquet")

    spark.read.parquet("/home/shunj/xad/study/111.parquet").show()

  }
}

