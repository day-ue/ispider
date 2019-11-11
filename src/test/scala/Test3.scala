import org.apache.spark.{SparkConf, SparkContext}

object Test3 {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setMaster("local[*]").setAppName("test")
    val sc=new SparkContext(conf)




    val list1=List((1,"2"),(1,"3"),(1,"4"),(1,"5"),(1,"1"),(2,"7"),(2,"8"),(2,"9"),(2,"18"),(2,"6"),(3,"1"),(3,"4"),(3,"qwertyui"))

    // 1  2,3,4,5,1
    //2   7 8 9 18
    // 3   1,4 ,qweqw

    val RDD1=sc.parallelize(list1)
    val tmp= RDD1.reduceByKey((k,v)=>v)
    tmp.foreach(print)

  }
}