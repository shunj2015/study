import org.apache.spark.{SparkConf, SparkContext}

object CollectionParallelize {
    def main(args:Array[String]): Unit = {
        val conf = new SparkConf().setAppName("cp").setMaster("master")
        val sc = new SparkContext(conf)
        val data = Array(1,2,3,4,5)
        val disdata = sc.parallelize(data)
        val count = disdata.reduce((a,b) => (a+b))
        System.out.println(count)
    }

}
