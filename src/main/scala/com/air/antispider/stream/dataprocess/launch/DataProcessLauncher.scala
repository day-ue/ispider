package com.air.antispider.stream.dataprocess.launch

import com.air.antispider.stream.common.bean._
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.common.util.log4j.LoggerLevels
import com.air.antispider.stream.dataprocess.businessprocess._
import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum.TravelTypeEnum
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/*
数据预处理总的程序入口
*/
object DataProcessLauncher {


  //程序入口
  def main(args: Array[String]): Unit = {
    //设置日志级别
    LoggerLevels.setStreamingLogLevels()
    //当应用被停止的时候，进行如下设置可以保证当前批次执行完之后再停止应用。
    System.setProperty("spark.streaming.stopGracefullyOnShutdown", "true")
    //spark conf 并开启任务集群
    val conf = new SparkConf().setMaster("local[2]").setAppName("DataProcess")
      .set("spark.metrics.conf.executor.source.jvm.class", "org.apache.spark.metrics.source.JvmSource") //并开启任务集群监控
    // spark context
    val sc = new SparkContext(conf)
    //调取读取kafka数据的API
    val kafkaParams = Map("metadata.broker.list" -> PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"))
    val topics = Set(PropertiesUtil.getStringByKey("source.nginx.topic", "kafkaConfig.properties"))
    //数据预处理程序的入口
    val ssc = setUp(sc, kafkaParams, topics)

    //开启spark streaming
    ssc.start()
    ssc.awaitTermination()

  }


  //数据预处理程序的入口
  def setUp(sc: SparkContext, kafkaParams: Map[String, String], topics: Set[String]) = {
    //spark streaming context
    val ssc = new StreamingContext(sc, Seconds(5))


    //读取数据库中的过滤规则
    var analyzeRuleDB = AnalyzeRuleDB.queryFilterRule()
    //将规则加载到广播变量
    @volatile var broadcastscanalyzeRuleDB = sc.broadcast(analyzeRuleDB)
    ////读取数据库中的飞行类型，操作类型规则
    var flightTypeExpression = AnalyzeRuleDB.queryRuleMap()
    @volatile var broadcastflightTypeExpression = sc.broadcast(flightTypeExpression)
    //数据解析规则--  查询类
    var queryRule = AnalyzeRuleDB.queryRule(0)
    @volatile var broadcastQueryRules = sc.broadcast(queryRule)
    //数据解析规则--  预定类
    var bookRule = AnalyzeRuleDB.queryRule(1)
    @volatile var broadcastBookRules = sc.broadcast(bookRule)
    //读取黑名单-高频 IP 的数据
    var blackIPList: ArrayBuffer[String] = AnalyzeRuleDB.getBlackIpDB()
    @volatile var broadcastBlackIPList = sc.broadcast(blackIPList)


    //获取redis集群
    val redis = JedisConnectionUtil.getJedisCluster

    //读取数据
    val getKafkaDara = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val KafkaDara = getKafkaDara.map(_._2)
    //消费数据（先打印）
    KafkaDara.foreachRDD(dataRDD => {
      //读取redis标识，看看是否需要更新规则
      val needUpdateFilterList = redis.get("FilterChangeFlag")
      //如果redis中标识过滤规则的标签不是空的，并且是真那么标识需要更新数据的规则到程序内，病加载到广播变量
      if (!needUpdateFilterList.isEmpty && needUpdateFilterList.toBoolean) {
        //重新读取mysql中的新规则
        analyzeRuleDB = AnalyzeRuleDB.queryFilterRule()
        //情况广播变量
        broadcastscanalyzeRuleDB.unpersist()
        //重新广播新规则
        broadcastscanalyzeRuleDB = sc.broadcast(analyzeRuleDB)
        //将标识改成false
        redis.set("FilterChangeFlag", "false")
      }
      //读取redis飞行类型，操作类型的标识，看看是否需要更新规则
      val NeedUpDateBlackIPList = redis.get("NeedUpDateBlackIPList")
      //若读到的标记不是空，并且是true
      if (!NeedUpDateBlackIPList.isEmpty && NeedUpDateBlackIPList.toBoolean) {
        //重新读取数据库规则
        flightTypeExpression = AnalyzeRuleDB.queryRuleMap()
        //清空广播变量
        broadcastflightTypeExpression.unpersist()
        //充新加载
        broadcastflightTypeExpression = sc.broadcast(flightTypeExpression)
        //将标记改成false
        redis.set("ClassifyRuleChangeFlag", "false")
      }

      /*
解析数据是否需要更新///////////////////////
*/
      val needUpDataAnalyzeRule = redis.get("NeedUpDataAnalyzeRule")
      //如果获取的数据是非空的，并且这个值是 true,那么就进行数据的更新操作（在数据库中重新读取数据加载到 redis）
      if (!needUpDataAnalyzeRule.isEmpty && needUpDataAnalyzeRule.toBoolean) {
        //重新读取 mysql 的数据
        queryRule = AnalyzeRuleDB.queryRule(0)
        bookRule = AnalyzeRuleDB.queryRule(1)
        //清空广播变量中的数据
        broadcastQueryRules.unpersist()
        broadcastBookRules.unpersist()
        //重新载入新的过滤数据
        broadcastQueryRules = sc.broadcast(bookRule)
        broadcastBookRules = sc.broadcast(bookRule)
        //更新完毕后，将 redis 中的 true  改成 false
        redis.set("AnalyzeRuleNeedUpData", "false")
      }
      /*
      黑名单-高频 IP 的数据是否需要更新
      */
      val needUpDateBlackIPList = redis.get("NeedUpDateBlackIPList")
      //如果获取的数据是非空的，并且这个值是 true,那么就进行数据的更新操作（在数据库中重新读取数据加载到 redis）
      if (!needUpDateBlackIPList.isEmpty && needUpDateBlackIPList.toBoolean) {
        //重新读取 mysql 的数据
        blackIPList = AnalyzeRuleDB.getBlackIpDB()
        //情况广播变量中的数据
        broadcastBlackIPList.unpersist()
        //重新载入新的过滤数据
        broadcastBlackIPList = sc.broadcast(blackIPList)
        //更新完毕后，将 redis 中的 true  改成 false
        redis.set("NeedUpDateBlackIPList", "false")
      }


      //第一个功能模块 链路统计功能
      val serverUserCount:collection.Map[String, Int]= LinkCount.getLinkCount(dataRDD)

      //1   数据过滤功能，将不要的数据过滤掉
      val filterData: RDD[String] = dataRDD.filter(message => URLFilter.filterURL(message, broadcastscanalyzeRuleDB.value))
      //filterData.foreach(println)

      //下面的操作不是单纯的脱敏
      //数据预处理的核心过程
      val dataPreProcess = filterData.map(message => {
        //2    数据脱敏方法  手机号脱敏
        val encryptedPhone: String = EncryptedData.encryptedPhone(message)
        //2    数据脱敏方法  身份证脱敏
        val encryptedID: String = EncryptedData.encryptedId(encryptedPhone)

        //3     数据拆分
        val (request, requestMethod, contentType, requestBody, httpReferrer, remoteAddr, httpUserAgent, timeIso8601, serverAddr, cookiesStr, cookieValue_JSESSIONID, cookieValue_USERID)
        = DataSplit.dataSplit(encryptedID)
        ////////////////////////////////////////////////////////////////////////////////////////////////
        //数据分类  查询、预定
        /*
       读取飞行类型，操作类型的规则
        */
        //数据分类  查询
        //飞行类型（0-国内，1-国际）     操作类型（0-查询，1-预定）
        //    0                                       0     （国内查询）
        //     0                                       1    （国内预定）
        //    1                                        0     （国际查询）
        //    1                                       1      （国际预定）

        //val aa =broadcastflightTypeExpression.value
        //4    requestType  返回的结果   International / National  , Query /  Book
        val requestType: RequestType = RequestTypeClassifier.classifyByRequest(request, broadcastflightTypeExpression.value)

        //5   往返类型
        val travelType: TravelTypeEnum = TravelTypeClassifier.classifyByRefererAndRequestBody(httpReferrer)

        //6   数据解析    查询  去数据库匹配出解析规则，用解析规则解析查询中的 body 数据
        val queryRequestData: Option[QueryRequestData] = AnalyzeRequest.analyzeQueryRequest(requestType, requestMethod, contentType, request, requestBody, travelType, broadcastQueryRules.value)

        //6    数据解析   预定
        val queryBookRequestData: Option[BookRequestData] = AnalyzeBookRequest.analyzeBookRequest(requestType, requestMethod, contentType, request, requestBody, travelType, broadcastQueryRules.value)

        //7  数据加工  判断是否是黑名单内的IP
        var highFrqIPGroup: Boolean = IpOperation.isFreIP(remoteAddr, broadcastBlackIPList.value)


        //8   数据结构化
        val processedData: ProcessedData = DataPackage.dataPackage("", requestMethod, request, remoteAddr, httpUserAgent, timeIso8601, serverAddr, highFrqIPGroup,
          requestType, travelType, cookieValue_JSESSIONID, cookieValue_USERID, queryRequestData, queryBookRequestData, httpReferrer)
        processedData


      })


      //9  数据推送dataPreProcess.foreach(println)
      //查询行为数据  for Kafka
      DataSend.sendQueryDataToKafka(dataPreProcess)
      //预订行为数据  for Kafka
      DataSend.sendBookDataToKafka(dataPreProcess)

      //10  系统监控
     SparkStreamingMonitor.streamMonitor(sc,dataRDD,serverUserCount,redis)

    })
    ssc
  }


}
