import org.apache.spark.{SparkConf, SparkContext}

object Broadcast {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("broadcast")
        val sc = new SparkContext(conf)
        val data = Array(1,2,3)
        val bc = sc.broadcast(data)
        bc.value.foreach(println)
        println("----------")
        println(bc.value)
    }
}
