package com.air.antispider.stream.rulecompute.launch

import java.text.SimpleDateFormat

import com.air.antispider.stream.common.bean.ProcessedData
import com.air.antispider.stream.common.util.hdfs.{BlackListToHDFS, BlackListToRedis}
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.common.util.kafka.KafkaOffsetUtil
import com.air.antispider.stream.common.util.log4j.LoggerLevels
import com.air.antispider.stream.dataprocess.businessprocess.{AnalyzeRuleDB, SparkStreamingMonitor}
import com.air.antispider.stream.rulecompute.businessprocess._
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/*
实时数据分析的程序入口
爬虫识别的程序入口
 */
object RuleComputLauncher {


  //程序入口
  def main(args: Array[String]): Unit = {
    //设置日志级别
    LoggerLevels.setStreamingLogLevels()
    //当应用被停止的时候，进行如下设置可以保证当前批次执行完之后再停止应用。
    System.setProperty("spark.streaming.stopGracefullyOnShutdown", "true")
    //spark conf 并开启任务集群
    val conf = new SparkConf().setMaster("local[2]").setAppName("RuleComput")
      .set("spark.metrics.conf.executor.source.jvm.class", "org.apache.spark.metrics.source.JvmSource") //并开启任务集群监控
    // spark context
    val sc = new SparkContext(conf)
    //调取读取kafka数据的API
    val kafkaParams = Map("metadata.broker.list" -> PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"))
    val topics = Set(PropertiesUtil.getStringByKey("source.query.topic", "kafkaConfig.properties"))

    //人为维护offsset 需要使用zk
    val zkHosts = PropertiesUtil.getStringByKey("zkHosts", "zookeeperConfig.properties")
    val zkoffsetPath = PropertiesUtil.getStringByKey("rulecompute.antispider.zkPath", "zookeeperConfig.properties")
    val zkClient = new ZkClient(zkHosts)


    //数据实时分析程序的入口（爬虫识别的入口）
    val ssc = setUps(sc, kafkaParams, topics, zkClient, zkHosts, zkoffsetPath)

    //开启spark streaming
    ssc.start()
    ssc.awaitTermination()

  }

  /*
  实时分析数据读取代码+分析
   */
  def setUps(sc: SparkContext, kafkaParams: Map[String, String], topics: Set[String], zkClient: ZkClient,
             zkHosts: String, zkoffsetPath: String) = {
    val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(5))
    //获取redis集群
    val jedis = JedisConnectionUtil.getJedisCluster
    //获取查询关键页面
    val queryCriticalPages: mutable.Seq[String] = AnalyzeRuleDB.queryCriticalPages()
    @volatile var broadcastQueryCriticalPages = sc.broadcast(queryCriticalPages)

    //获取流程规则策略配置
    val flowList = AnalyzeRuleDB.createFlow(0)
    @volatile var broadcastFlowList = sc.broadcast(flowList)


    //在ZK读取offset
    val offset = KafkaOffsetUtil.readOffsets(zkClient, zkHosts, zkoffsetPath, topics.last)

    //根据offset  读取kafka的数据
    val tmpKafkaData = offset match {
      case None => KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      case Some(fromoffset) =>
        val messageh = (mmh: MessageAndMetadata[String, String]) => (mmh.key(), mmh.message())
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromoffset, messageh)
    }
    val kafkaData: DStream[String] = tmpKafkaData.map(_._2)

    //使用“#CS#”将数据拆分开，封装成processedData
    val processedData: DStream[ProcessedData] = QueryDataPackage.queryDataLoadAndPackage(kafkaData)

    processedData.foreachRDD(rdd => {
      //询问redis 是否需要更新规则
      //关键页面是否改变标识
      val needUpdateQueryCriticalPages = jedis.get("QueryCriticalPagesChangeFlag")
      //黑名单是否改变标识
      val needUpdateBlackList = jedis.get("BlackChangeFlag")
      //流程规则策略变更标识
      val needUpdateRuleList = jedis.get("ProcessChangeFlag")
      if (!needUpdateQueryCriticalPages.isEmpty() &&
        needUpdateQueryCriticalPages.toBoolean) {
        val queryCriticalPagesUpdate = AnalyzeRuleDB.queryCriticalPages()
        broadcastQueryCriticalPages.unpersist()
        broadcastQueryCriticalPages = sc.broadcast(queryCriticalPagesUpdate)
        jedis.set("QueryCriticalPagesChangeFlag", "false")
      }

      //Mysql-流程规则策略是否改变标识
      if (!needUpdateRuleList.isEmpty() && needUpdateRuleList.toBoolean) {
        val flowListLast = AnalyzeRuleDB.createFlow(0)
        broadcastFlowList.unpersist()
        broadcastFlowList = sc.broadcast(flowListLast)
        jedis.set("ProcessChangeFlag", "false")
      }

    })


    //消费数据（爬虫识别的代码）
    //1      5 分钟内的 IP 段（IP 前两位）访问量
    val ipblock: DStream[(String, Int)] = CoreRule.ipBlockAccessCounts(processedData)
    //ipblock.foreachRDD(X=>X.foreach(println))
    //将结果集转换成Map,以便于后续使用
    var ipblockMap: collection.Map[String, Int] = null
    ipblock.foreachRDD(rdd => {
      ipblockMap = rdd.collectAsMap()
    })

    //2  某个 IP，5 分钟内总访问量
    val ipCount: DStream[(String, Int)] = CoreRule.ipAccessCounts(processedData)
    //ipCount.foreachRDD(X=>X.foreach(println))
    //将结果集转换成Map,以便于后续使用
    var ipCountMap: collection.Map[String, Int] = null
    ipCount.foreachRDD(rdd => {
      ipCountMap = rdd.collectAsMap()
    })

    //3     某个 IP，5 分钟内的关键页面访问总量
    val criticalPagesCount: DStream[(String, Int)] = CoreRule.criticalPagesCounts(processedData, broadcastQueryCriticalPages.value)
    // criticalPagesCount.foreachRDD(X=>X.foreach(println))
    //将结果集转换成Map,以便于后续使用
    var criticalPagesCountMap: collection.Map[String, Int] = null
    criticalPagesCount.foreachRDD(rdd => {
      criticalPagesCountMap = rdd.collectAsMap()
    })

    // 4   某个 IP，5 分钟内的 UA 种类数统计
    val userAgents = CoreRule.userAgent(processedData)
    //userAgents .foreachRDD(X=>X.foreach(println))
    //将结果集转换成Map,以便于后续使用
    var userAgentsMap: collection.Map[String, Int] = null
    userAgents.foreachRDD(rdd => {
      userAgentsMap = rdd.collectAsMap()
    })

    //5    某个 IP，5 分钟内查询不同行程的次数
    val depArrCount: DStream[(String, Int)] = CoreRule.flightQuerys(processedData)
    //depArrCount .foreachRDD(X=>X.foreach(println))
    //将结果集转换成Map,以便于后续使用
    var depArrCountMap: collection.Map[String, Int] = null
    depArrCount.foreachRDD(rdd => {
      depArrCountMap = rdd.collectAsMap()
    })

    //6   某个 IP，5 分钟内关键页面的访问 的 Cookie 数
    val criticalCookie: DStream[(String, Int)] = CoreRule.criticalCookies(processedData, broadcastQueryCriticalPages.value)
    //criticalCookie.foreachRDD(X=>X.foreach(println))
    //将结果集转换成Map,以便于后续使用
    var criticalCookieMap: collection.Map[String, Int] = null
    criticalCookie.foreachRDD(rdd => {
      criticalCookieMap = rdd.collectAsMap()
    })

    //7   某个 IP，5 分钟内的关键页面最短访问 间隔
    val mintime: DStream[(String, Int)] = CoreRule.criticalPagesAccTime(processedData, broadcastQueryCriticalPages.value)

    //将结果集转换成Map,以便于后续使用
    var mintimeMap: collection.Map[String, Int] = null
    mintime.foreachRDD(rdd => {
      mintimeMap = rdd.collectAsMap()
    })

    //8   某个 IP， 5 分钟内小于最短访问间隔（自设）的关键页面查询次数
    val aCriticalPagesAccTime = CoreRule.aCriticalPagesAccTime(processedData, broadcastQueryCriticalPages.value)

    //将结果集转换成Map,以便于后续使用
    var aCriticalPagesAccTimeMap: collection.Map[String, Int] = null
    aCriticalPagesAccTime.foreachRDD(rdd => {
      aCriticalPagesAccTimeMap = rdd.collectAsMap()
    })


    //将原始数据中的每个数据与前面算出的8个结果集进行一一匹配，
    // 获取到这一条数据在八个结果集中的对应的值
    //对这一条数据是否是爬虫数据进行分析
    //processedData
    //datas是识别爬虫后的数据（可能有爬虫数据，可能有非爬虫数据，每种数据都有重复数据）
    val datas: DStream[AntiCalculateResult] = processedData.map(processeddata => {
      val ip = processeddata.remoteAddr
      //为打分准备数据   返回结果最终打分后的case class   AntiCalculateResult
      val antiCalculateResul: AntiCalculateResult = RuleUtil.calculateAntiResult(processeddata,
        ip,
        ipblockMap,
        ipCountMap,
        criticalPagesCountMap,
        userAgentsMap,
        depArrCountMap,
        criticalCookieMap,
        mintimeMap,
        aCriticalPagesAccTimeMap,
        broadcastFlowList.value)
      antiCalculateResul
    })

    // datas.foreachRDD(X=>X.foreach(println))
    //将分析完的数据进行过滤，过滤掉不是爬虫的数据  剩下多有的爬虫数据
    //allSpiderData 是有重复数据的
    val allSpiderData: DStream[AntiCalculateResult] = datas.filter(antiCalculateResult => {
      //默认都不是爬虫
      var flag: Boolean = false
      //获得到flowsScores ：Array[FlowScoreResult]
      val flowsScores: Array[FlowScoreResult] = antiCalculateResult.flowsScore
      //遍历灭每一个flowsScores获取出他的 isUpLimited（是否是爬虫)
      for (flowsScore <- flowsScores) {
        if (flowsScore.isUpLimited) {
          flag = true
        }
      }
      flag
    })

    //将allSpiderData过来后的所有的爬虫进行去重
    val distentSpiderData: DStream[(String, Array[FlowScoreResult])] = allSpiderData.map(antiCalculateResult => {
      val ip: String = antiCalculateResult.ip
      val flowsScore = antiCalculateResult.flowsScore
      (ip, flowsScore)
    }).reduceByKey((k, v) => v)




    //将数据从写入redis
    distentSpiderData.foreachRDD(spiderDataRdd => {
      /*
      *恢复 redis 黑名单数据，用于防止程序停止而产生的 redis 数据丢失
      */
      BlackListToRedis.blackListDataToRedis(jedis, sc, sqlContext)
      //spiderDataRdd : RDD[(String, Array[FlowScoreResult])]
      //将spiderDataRdd转换成Array
      val distentSpiders: Array[(String, Array[FlowScoreResult])] = spiderDataRdd.collect()
      //遍历转换后的distentSpiders
      for (distentSpider <- distentSpiders) {
        //获取IP
        val ip: String = distentSpider._1
        //获取flowsScore: Array[FlowScoreResult]
        val flowsScores: Array[FlowScoreResult] = distentSpider._2
        //>>>>>>>>>>>>>>>>黑名单 DataFrame-备份到 HDFS
        val antiBlackListDFR = new ArrayBuffer[Row]()
        //遍历flowsScores  flowsScores是一个Array
        for (flowsScore <- flowsScores) {
          //准备写入redis的数据
          /*
          实际工作中的key  value  存储周期由企业自己设定
          我们这里为了看效果做了拼装
           */
          //去读key的前缀
          val keyPre = PropertiesUtil.getStringByKey("cluster.key.anti_black_list", "jedisConfig.properties")
          //组装key = 前缀+IP+流程IP+时间戳
          val key = keyPre + "|" + ip + "|" + flowsScore.flowId + System.currentTimeMillis().toString
          //去读数据的有效周期
          val time = PropertiesUtil.getStringByKey("cluster.exptime.ocp_black_list", "jedisConfig.properties").toInt
          //组装value =最终分数+命中的规则id+命中的时间
          val value = flowsScore.flowScore + "|" + flowsScore.hitRules.toString() + "|" + flowsScore.hitTime
          //写入redis
          jedis.setex(key, time, value)
          //添加黑名单 DataFrame-备份到 ArrayBuffer
          antiBlackListDFR.append(Row(((PropertiesUtil.getStringByKey("cluster.exptime.anti_black_list",
            "jedisConfig.properties").toLong) * 1000 + System.currentTimeMillis()).toString, key,
            value))

        }
        /*
        *增加黑名单数据实时存储到 HDFS 的功能-黑名单数据持久化-用于 Redis 数据恢
        复
        */
        BlackListToHDFS.saveAntiBlackList(sc.parallelize(antiBlackListDFR), sqlContext)

      }
    })


    /*
    *存储规则计算结果 RDD（antiCalculateResults）  到 HDFS
    * kafkaData是在数据预处理后的topic中读取的数据
    */
    kafkaData.foreachRDD(antiCalculateResult => {
      val date: String = new SimpleDateFormat("yyyy/MM/dd/HH").format(System.currentTimeMillis())
      val yyyyMMddHH = date.replace("/", "").toInt
      val path: String =
        PropertiesUtil.getStringByKey("blackListPath", "HDFSPathConfig.properties") + "itcast/" + yyyyMMddHH
      try {
        sc.textFile(path + "/part-00000").union(antiCalculateResult).repartition(1).saveAsTextFile(path)
      } catch {
        case e: Exception =>
          antiCalculateResult.repartition(1).saveAsTextFile(path)
      }
      //设置任务监控
      SparkStreamingMonitor.queryMonitor(sc, antiCalculateResult)
    })



    //维护zk offset   （更新offset）
    tmpKafkaData.foreachRDD(rdd => {
      KafkaOffsetUtil.saveOffsets(zkClient, zkHosts, zkoffsetPath, rdd)
    })

    ssc
  }

}
