import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("networdcount")
    val ssc = new StreamingContext(conf, Seconds[1])
    val lines = ssc.socketTextStream("localhost", 9999)
    val counts = lines.flatMap(line=>line.split(" "))
      .map(word=>(word,1))
      .reduceByKey((a,b)=>(a+b))
    counts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
