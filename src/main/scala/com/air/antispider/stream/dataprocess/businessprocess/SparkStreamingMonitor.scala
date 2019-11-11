package com.air.antispider.stream.dataprocess.businessprocess

import java.text.SimpleDateFormat
import java.util.Date

import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.common.util.spark.SparkMetricsUtils
import com.alibaba.fastjson.JSONObject
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import redis.clients.jedis.JedisCluster

/*
数据预处理的效率计算
各个链路数据的统计
 */
object SparkStreamingMonitor {
  def streamMonitor(sc: SparkContext, dataRDD: RDD[String], serverUserCount: collection.Map[String, Int], redis: JedisCluster): Unit = {

    //一个RDD内的数据的数量和
    val dataCount=dataRDD.count()
    //获取任务的ID
    val appid=sc.applicationId
    //获取任务的name
    val appName=sc.appName
    val endTimePath=appid+".driver."+appName+".StreamingMetrics.streaming.lastCompletedBatch_processingEndTime"
    val startTimePath=appid+".driver."+appName+".StreamingMetrics.streaming.lastCompletedBatch_processingStartTime"
    val url="http://localhost:4040/metrics/json/"
    //数据处理的时间   通过路径 http://localhost:4040/metrics/json/ 获取
    val jsonOb: JSONObject =SparkMetricsUtils.getMetricsJson(url)
    val gaugesOb=jsonOb.getJSONObject("gauges")
    //获取结束时间
    val endTimevalue= gaugesOb.getJSONObject(endTimePath)
    var endTimestamp:Long=0
    if (endTimevalue!=""){
      endTimestamp= endTimevalue.getLong("value")
    }
    //开始的时间
    val startTimevalue= gaugesOb.getJSONObject(startTimePath)
    var startTimestamp:Long=0
    if (startTimevalue!=""){
      startTimestamp= startTimevalue.getLong("value")
    }
    //结束的时间-开始的时间  =任务执行时间
    var runTime=endTimestamp-startTimestamp
    //数量  除以 时间=数据处理速度
    //计算数据处理的速度
    //当执行时间大于0时，继续分析
    if(runTime>0){
      //将结束时间的时间戳转换成年月日时分秒
      val format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
     val endTime= format.format(new Date(endTimestamp))
      var runSpeed:Double=dataCount/runTime
      val Maps=Map(
        "costTime"->runTime.toString,
        "serversCountMap"->serverUserCount,
        "applicationId"->appid.toString,
        "countPerMillis"->runSpeed.toString,
        "applicationUniqueName"->appName.toString,
        "endTime"->endTime.toString,
        "sourceCount"->dataCount.toString
      )

      val key =PropertiesUtil.getStringByKey("cluster.key.monitor.dataProcess","jedisConfig.properties")+System.currentTimeMillis()
      val time=PropertiesUtil.getStringByKey("cluster.exptime.monitor","jedisConfig.properties").toInt
      //将数据写入redis
      redis.setex(key,time,Json(DefaultFormats).write(Maps))

    }
  }





  //实时分析的性能计算
  def queryMonitor(sc: SparkContext, antiCalculateResult: RDD[String]): Unit = {
    //done:Spark 性能实时监控
    //监控数据获取
    val sourceCount = antiCalculateResult.count()
    //  val  sparkDriverHost  =
    //sc.getConf.get("spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES")
      //监控信息页面路径为集群路径+/proxy/+应用 id+/metrics/json
      //val url = s"${sparkDriverHost}/metrics/json"
      //local 模式的路径
      val url = "http://localhost:4041/metrics/json/"
      val jsonObj = SparkMetricsUtils.getMetricsJson(url)
      //应用的一些监控指标在节点 gauges 下
      val result = jsonObj.getJSONObject("gauges")
      //监控信息的 json 路径：应用 id+.driver.+应用名称+具体的监控指标名称
      //最近完成批次的处理开始时间-Unix 时间戳（Unix timestamp）-单位：毫秒
      val  startTimePath  =  sc.applicationId  +  ".driver."  +  sc.appName +".StreamingMetrics.streaming.lastCompletedBatch_processingStartTime"
      val startValue = result.getJSONObject(startTimePath)
      var processingStartTime: Long = 0
      if (startValue != null) {
      processingStartTime = startValue.getLong("value")
      }
      //最近完成批次的处理结束时间-Unix 时间戳（Unix timestamp）-单位：毫秒
      val  endTimePath  =  sc.applicationId  +  ".driver."  +  sc.appName  +".StreamingMetrics.streaming.lastCompletedBatch_processingEndTime"
      val endValue = result.getJSONObject(endTimePath)
      var processingEndTime: Long = 0
      if (endValue != null) {
      processingEndTime = endValue.getLong("value")
      }
      //流程所用时间
      val processTime = processingEndTime - processingStartTime
      //监控数据推送
      //done:实时处理的速度监控指标-monitorIndex 需要写入 Redis，由 web 端读取 Redis 并持
      //久化到 Mysql
      val endTime = processingEndTime
      val costTime = processTime
      val countPerMillis = sourceCount.toFloat / costTime.toFloat
      val  monitorIndex  =  (endTime,  "RuleComput",  sc.applicationId,  sourceCount,  costTime,countPerMillis)

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val processingEndTimeString = format.format(new Date(processingEndTime))
      val fieldMap = scala.collection.mutable.Map(
      "endTime" -> processingEndTimeString,
      "applicationUniqueName" -> monitorIndex._2.toString,
      "applicationId" -> monitorIndex._3.toString,
      "sourceCount" -> monitorIndex._4.toString,
      "costTime" -> monitorIndex._5.toString,
      "countPerMillis" -> monitorIndex._6.toString,
      "serversCountMap" -> Map[String, Int]())
      //将数据存入 redis 中
      try {
      val jedis = JedisConnectionUtil.getJedisCluster
      //监控 Redis 库
      //jedis.select(redis_db_monitor);
      //保存监控数据
      //产生不重复的 key 值
      val  keyName  =  PropertiesUtil.getStringByKey("cluster.key.monitor.query","jedisConfig.properties") + System.currentTimeMillis.toString

      jedis.setex(keyName,  PropertiesUtil.getStringByKey("cluster.exptime.monitor","jedisConfig.properties").toInt, Json(DefaultFormats).write(fieldMap))
      //更新最新的监控数据
     // jedis.setex(keyNameLast,  PropertiesUtil.getStringByKey("cluster.exptime.monitor","jedisConfig.properties").toInt, Json(DefaultFormats).write(fieldMap))
      //JedisConnectionUtil.returnRes(jedis)
      } catch {
      case e: Exception =>
      e.printStackTrace()
      }
      }


}
