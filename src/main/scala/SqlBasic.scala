import org.apache.spark.sql.SparkSession
object SqlBasic {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("spark sql basic")
      .config("spark.some.config.option","some-value")
      .getOrCreate()
    val df = spark.read.json("/home/shunj/xad/study/resources/sql/people.json")
    import spark.implicits._

    df.show()
    df.printSchema()
    df.select("name").show()
    df.select("name", "age").show()
    df.filter($"age">20).show()
  }
}
