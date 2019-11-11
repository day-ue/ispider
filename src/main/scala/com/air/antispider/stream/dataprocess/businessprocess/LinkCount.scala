package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

/*
进行链路统计的代码实现
 */
object LinkCount {
  //链路统计功能实现
  def getLinkCount(dataRDD: RDD[String]):collection.Map[String, Int]= {
    //一个批次服务器访问总量的统计
    //便利RDD内的没有个数据
    val serverUserCount=dataRDD.map(message=>{
      var serverIP=""
      //若数据使用#CS#分割后的长度大于9，也就是有第十个值，获取第十个值
      if (message.split("#CS#").length>9){
        //将第十个值赋值给serverIP
        serverIP=message.split("#CS#")(9)
      }
      //返回serverIP和1   为reduceByKey提供数据
      (serverIP,1)
    }).reduceByKey(_+_)//求相同serverIP的总量



    //服务器当前活跃链接数统计
    //便利RDD内的每一条数据
    val activeuser=dataRDD.map(message=>{
      //初始化默认的serverIP和活跃链接数
      var serverIP=""
      var activeuser=""
      //若数据使用#CS#分割后的长度大于11，也就是有第12个值，获取第十个值作为IP  取第12个值作为活跃链接数
      if (message.split("#CS#").length>11){
        //截取第10个数据赋值给serverIP
        serverIP=message.split("#CS#")(9)
        //截取第12个数据赋值给activeuser
        activeuser=message.split("#CS#")(11)
      }
      //返回serverIP 和活跃链接数
      (serverIP,activeuser)
    }).reduceByKey((k,v)=>v)// 取出每个serverIP对应的value List内的最后一个活跃链接数


    //将上面的两个数据写入redis
    //将数据转换成MAP
    val serverUserCountMap=serverUserCount.collectAsMap()
    val activeuserMap=activeuser.collectAsMap()

      if (!serverUserCountMap.isEmpty && !activeuserMap.isEmpty ){
        //获取redis 集群
        val redis =JedisConnectionUtil.getJedisCluster
        //读取链路数据的key 的前缀（后缀时时间戳）
        val key =PropertiesUtil.getStringByKey("cluster.key.monitor.linkProcess","jedisConfig.properties")+System.currentTimeMillis()
        val time=PropertiesUtil.getStringByKey("cluster.exptime.monitor","jedisConfig.properties").toInt

        //封装前面算完的数据(两个小MAP)，封装成大MAP
         val Maps=Map(
           "serversCountMap"->serverUserCountMap,
          "activeNumMap"->activeuserMap
         )

        //将数据写入redis
        redis.setex(key,time,Json(DefaultFormats).write(Maps))

      }
    serverUserCount.collectAsMap()
  }


}
