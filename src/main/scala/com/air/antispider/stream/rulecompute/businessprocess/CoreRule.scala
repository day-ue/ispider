package com.air.antispider.stream.rulecompute.businessprocess

import java.text.SimpleDateFormat

import com.air.antispider.stream.common.bean.ProcessedData
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable.ArrayBuffer


/*
八项规则的分析
 */
object CoreRule {

  def aCriticalPagesAccTime(processedDatas: DStream[ProcessedData], values: ArrayBuffer[String]): DStream[(String, Int)]  = {
    val defaultMinTime=5000
    //用来标记小于自设值的储量（defaultMinTime=5000）
    var count=0
    //拿数据的url和规则进行匹配
    //遍历processedDatas内的每个数据（ProcessedData）
    val ipTimeList: DStream[(String, Iterable[String])]=processedDatas.map(processedData=>{

      //默认全部都不是关键页面
      var flag =false
      //获取ip和URL和时间
      val ip=processedData.remoteAddr
      val url=processedData.request
      val time=processedData.timeIso8601
      //将数据的url和规则进行匹配
      for(value<-values){
        //如果url不配上了规则，那么标识这个url是关键页面
        if (url.matches(value)){
          flag =true
        }
      }
      //返回关键页面的ip和1
      if(flag){
        (ip,time)
      }else{
        ("","")
      }
    }).groupByKey()////返回命中关键页面的ip 和 对应的时间的list

    //某一个ip 和ip对应的小于自设值的的数量
    val ipcount: DStream[(String, Int)] =ipTimeList.map(iptimelists=>{
      val ip=iptimelists._1
      //拿到时间点的list【#2019-03-25T09:17:58+08:00#】
      //                  #2019-03-20T07:12:04-07:00#CS
      val timeList: Iterable[String]=iptimelists._2


      //将timeIso8601类型的时间转换成时间戳
      //tmptime -> 【#2019-03-25T09:17:58+08:00#】
      val Sim=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //定义一个Long类型的list
      val timeLongList=new java.util.ArrayList[Long]()
      //val timeLongList=new  ArrayBuffer[Long]()
      for(tmptime<-timeList){
        //2019-03-25 09:17:58
        val  time:String=tmptime.substring(0,tmptime.indexOf("+")).replace("T"," ")
        //将time转换成时间戳
        val timeLong= Sim.parse(time).getTime
        timeLongList.add(timeLong)
        // timeLongList+=timeLong
      }
      //将timeLongList转换成Array
      val  timeLongListArr=timeLongList.toArray
      //进行排序
      java.util.Arrays.sort(timeLongListArr)

      //求每两个之间的时间差
      for(i<- 1 until timeLongListArr.length){
        //求相邻两个时间的时间差
        val timeD: Long= timeLongListArr(i).toString.toLong-timeLongListArr(i-1).toString.toLong
        //将两个时间的差与自设值对比  若小于自设值则count+1
        if (defaultMinTime>timeD){
          count+=1
        }

      }

      (ip,count)
    })
    ipcount
  }

  //某个 IP，5 分钟内的关键页面最短访问间隔
  def criticalPagesAccTime(processedDatas: DStream[ProcessedData], values: ArrayBuffer[String]): DStream[(String, Int)]  = {

    //拿数据的url和规则进行匹配
    //遍历processedDatas内的每个数据（ProcessedData）
    val ipTimeList: DStream[(String, Iterable[String])]=processedDatas.map(processedData=>{
      //默认全部都不是关键页面
      var flag =false
      //获取ip和URL和时间
      val ip=processedData.remoteAddr
      val url=processedData.request
      val time=processedData.timeIso8601
      //将数据的url和规则进行匹配
      for(value<-values){
        //如果url不配上了规则，那么标识这个url是关键页面
        if (url.matches(value)){
          flag =true
        }
      }
      //返回关键页面的ip和1
      if(flag){
        (ip,time)
      }else{
        ("","")
      }
    }).groupByKey()////返回命中关键页面的ip 和 对应的时间的list

    //某一个ip 和ip对应的时间的list
    val mintime: DStream[(String, Int)] =ipTimeList.map(iptimelists=>{
      val ip=iptimelists._1
      //拿到时间点的list【#2019-03-25T09:17:58+08:00#】
      //                  #2019-03-20T07:12:04-07:00#CS
      val timeList: Iterable[String]=iptimelists._2
       if (timeList.size>1){
         //将timeIso8601类型的时间转换成时间戳
         //tmptime -> 【#2019-03-25T09:17:58+08:00#】
         val Sim=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
         //定义一个Long类型的list
         val timeLongList=new java.util.ArrayList[Long]()
         //val timeLongList=new  ArrayBuffer[Long]()
         for(tmptime<-timeList){
           //2019-03-25 09:17:58
           val  time:String=tmptime.substring(0,tmptime.indexOf("+")).replace("T"," ")
           //将time转换成时间戳
           val timeLong= Sim.parse(time).getTime
           timeLongList.add(timeLong)
           // timeLongList+=timeLong
         }
         //将timeLongList转换成Array
         val  timeLongListArr=timeLongList.toArray
         //进行排序
         java.util.Arrays.sort(timeLongListArr)
         //存储时间差的List
         //val timeDList=new java.util.ArrayList[Long]()
         val timeDList=new  ArrayBuffer[Long]()
         //求每两个之间的时间差
         for(i<- 1 until timeLongListArr.length){
           //求相邻两个时间的时间差
           val timeD: Long= timeLongListArr(i).toString.toLong-timeLongListArr(i-1).toString.toLong
           //将每一个时间差写入timeDList
           timeDList+=(timeD)
         }
         //对时间差进行排序

         //将时间差的List转换成Array
         val  timeDListArr=timeDList.toArray
         //对时间差的List进行排序
         java.util.Arrays.sort(timeDListArr)
         //将ip，和这个ip对应的此批数据最小时间差返回
         (ip,timeDListArr(0).toInt)
       }else{
         ("",0)
       }


    })
    mintime
  }

  //某个 IP，5 分钟内关键页面的访问 的 Cookie 数
  def criticalCookies(processedDatas: DStream[ProcessedData], values: ArrayBuffer[String]): DStream[(String, Int)]  = {
    //遍历processedDatas内的每个数据（ProcessedData）
    val cookieCounts: DStream[(String, Iterable[String])]=processedDatas.map(processedData=>{
      //默认全部都不是关键页面
      var flag =false
      //获取ip和URL
      val ip=processedData.remoteAddr
      val url=processedData.request
      val cook=processedData.cookieValue_USERID
      //将数据和规则进行匹配
      for(value<-values){
        //如果url不配上了规则，那么标识这个url是关键页面
        if (url.matches(value)){
          flag =true
        }
      }
      //返回关键页面的ip和1
      if(flag){
        (ip,cook)
      }else{
        ("","")
      }
    }).groupByKey()


     val cookieSize: DStream[(String, Int)]  =cookieCounts.map(ipcookies=>{
      //获取ip
      val ip=ipcookies._1
      //获取dcookie的list
      val cookieList=ipcookies._2
      //对cookie的list去重并且求大小（最终结果）
      val cookiesize=cookieList.toList.distinct.size
      //返回ip和最终结果cookie的size
      (ip,cookiesize)
    })
    cookieSize
  }

  //某个 IP，5 分钟内查询不同行程的次数
  def flightQuerys(processedDatas: DStream[ProcessedData]): DStream[(String, Int)] = {
    //遍历processedDatas内的每个数据（ProcessedData）
    val deparrs: DStream[(String, Iterable[String])]=processedDatas.map(processedData=>{
      //获取ip和出发地  目的地
      val ip: String =processedData.remoteAddr
      val dep: String =processedData.requestParams.depcity
      val arr: String =processedData.requestParams.arrcity
      //将出发地和目的地拼接在一起
      val deparr: String =dep+arr
      //输出ip和出发地和目的地拼接后的结果
      (ip,deparr)
    }).groupByKey()//获得到ip和    useragent的list

    //遍历userAgentList
    val  depArrSize: DStream[(String, Int)] =deparrs.map(deparr=>{
      //获取ip
      val ip=deparr._1
      //获取dep+arr的list
      val deparrlist=deparr._2
      //对dep+arr的list去重并且求大小（最终结果）
      val deparrsize=deparrlist.toList.distinct.size
      //返回ip和最终结果dep+arr的size
      (ip,deparrsize)
    })
    depArrSize
  }




  //某个 IP，5 分钟内的 UA 种类数统计
  def userAgent(processedDatas: DStream[ProcessedData]): DStream[(String, Int)]  = {
    //遍历processedDatas内的每个数据（ProcessedData）
     val userAgentList: DStream[(String, Iterable[String])]=processedDatas.map(processedData=>{
       //获取ip和useragent
      val ip=processedData.remoteAddr
      val useragent=processedData.httpUserAgent
       //输出ip和useragent
      (ip,useragent)
    }).groupByKey()//获得到ip和    useragent的list

    //遍历userAgentList
   val  ipAgentSize: DStream[(String, Int)] =userAgentList.map(useragentlists=>{
     //获取ip
      val ip=useragentlists._1
     //获取useragent的list
      val agentlist=useragentlists._2
     //对useragent的list去重并且求大小（最终结果）
      val agentsize=agentlist.toList.distinct.size
     //返回ip和最终结果agentsize
      (ip,agentsize)
    })
    ipAgentSize
  }

  def criticalPagesCounts(processedDatas: DStream[ProcessedData], values: ArrayBuffer[String]): DStream[(String, Int)] = {
    //遍历processedDatas内的每个数据（ProcessedData）
    val criticalPagesCounts=processedDatas.map(processedData=>{
      //默认全部都不是关键页面
      var flag =false
      //获取ip和URL
      val ip=processedData.remoteAddr
      val url=processedData.request
      //将数据和规则进行匹配
      for(value<-values){
        //如果url不配上了规则，那么标识这个url是关键页面
        if (url.matches(value)){
          flag =true
        }
      }
      //返回关键页面的ip和1
      if(flag){
        (ip,1)
      }else{
        (ip,0)
      }
    }).reduceByKey(_+_)//求相同ip关键页面的总和
    criticalPagesCounts
  }





  def ipAccessCounts(processedDatas: DStream[ProcessedData]): DStream[(String, Int)] = {
    val ipCount=processedDatas.map(processedData=>{
      val ip=processedData.remoteAddr
      (ip,1)
    }).reduceByKey(_+_)
    ipCount

  }


  //5 分钟内的 IP 段（IP 前两位）访问量
  def ipBlockAccessCounts(processedDatas: DStream[ProcessedData]) : DStream[(String, Int)] = {
    val ipblock: DStream[(String, Int)] =processedDatas.map(processedData=>{
      //192.168.56.151
     val ip: String= processedData.remoteAddr
      val first=ip.indexOf(".")
      val secondary=ip.indexOf(".",first+1)
      val ipblock=ip.substring(0,secondary)
      (ipblock,1)
    }).reduceByKey(_+_)
    ipblock
  }

  def ipBlockAccessCounts_b(processedDatas: DStream[ProcessedData]) : DStream[(String, Int)] = {
    val ipblock=processedDatas.map( processedData=>{
      //192.168.56.151
      val ip: String= processedData.remoteAddr
      val first=ip.indexOf(".")
      val secondary=ip.indexOf(".",first+1)
      val ipblock=ip.substring(0,secondary)
      (ipblock,1)
    }).reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(5),Seconds(5))
    ipblock
  }

}
