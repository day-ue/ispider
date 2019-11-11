import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import com.air.antispider.stream.common.util.log4j.LoggerLevels
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TestKafkaS {
  //程序的入口
  def main(args: Array[String]): Unit = {
    //设置日志级别
    LoggerLevels.setStreamingLogLevels()
    //实例化SPARK conf
    val conf = new SparkConf().setMaster("local[2]").setAppName("TestCreateDirectStream")
    //实例sparkcontext
    val sc = new SparkContext(conf)
    //SSC
    val ssc = new StreamingContext(sc, Seconds(2))
    //createDirectStream
    //    // jssc: JavaStreamingContext,
    //      zkQuorum: String,
    //      groupId: String,
    //      topics: JMap[String, JInt]
    val zk = PropertiesUtil.getStringByKey("zkHosts", "zookeeperConfig.properties")
    val topics = Map(PropertiesUtil.getStringByKey("source.nginx.topic", "kafkaConfig.properties") -> 1)


    val getKafkaData = KafkaUtils.createStream(ssc, zk, "test",topics)
    val KafkaData = getKafkaData.map(_._2)
    //输出
        KafkaData.foreachRDD(RDD=>RDD.foreach(message=>{
          println(">>>>   "+message)
        }))
    //程序启动
    ssc.start()
    ssc.awaitTermination()
  }
}
