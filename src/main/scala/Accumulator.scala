import org.apache.spark.{SparkConf, SparkContext}

object Accumulator {
    def main(args:Array[String]): Unit = {
      val conf = new SparkConf().setAppName("accu")
      val sc = new SparkContext(conf)
      val accum = sc.longAccumulator("my acc")
      val distData = sc.parallelize(Array(1,2,3))
      distData.foreach(x=> accum.add(x))
      println(accum.value)
    }
}
