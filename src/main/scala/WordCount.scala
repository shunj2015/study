import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("wordCount")
        val sc = new SparkContext(conf)
        val distFile = sc.textFile("/home/shunj/xad/study/resources/wordCount")
        val counts = distFile
          .flatMap(line => line.split(" "))
          .map(word=>(word,1)).reduceByKey((a,b)=>a+b)
        System.out.println(counts.collect())
        counts.saveAsTextFile("/home/shunj/xad/study/resources/wordCount/output")
    }
}
