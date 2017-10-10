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


    println("-------temp view----------")
    df.createOrReplaceTempView("people")
    spark.sql("select * from people").show()

    println("--------global view---------")
    df.createGlobalTempView("gpeople")
    spark.sql("select * from global_temp.gpeople").show()
    spark.newSession().sql("select * from global_temp.gpeople").show()



    println("---------datasets--------")

    case class Person(name:String, age:Long)

    val ds = Seq(Person("jim", "18")).toDS()
    ds.show()
    val ds2 = Seq(1,2,3).toDS().map(_+1).collect()

    val dsp = spark.read.json("/home/shunj/xad/study/resources/sql/people.json").as[Person]
    dsp.show()

  }
}
