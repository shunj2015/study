import org.apache.spark.{SparkConf, SparkContext}

object ExternalDataSets {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("External")
        val sc = new SparkContext(conf)
        val distFile = sc.textFile("/home/shunj/xad/study/resources/external")
        val count = distFile.map(line=>line.length).reduce((a,b)=>a+b)
        System.out.println(count)
    }
}
