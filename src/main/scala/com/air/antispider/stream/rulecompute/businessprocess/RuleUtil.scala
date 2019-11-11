package com.air.antispider.stream.rulecompute.businessprocess

import java.util.Date

import com.air.antispider.stream.common.bean.{FlowCollocation, ProcessedData, RuleCollocation}

import scala.collection.mutable.ArrayBuffer

/*
实际打分算法
 */
object RuleUtil {


  //为打分准备数据
  def calculateAntiResult(processeddata: ProcessedData, ip: String, ipblockMap: collection.Map[String, Int],
                          ipCountMap: collection.Map[String, Int], criticalPagesCountMap: collection.Map[String, Int],
                          userAgentsMap: collection.Map[String, Int], depArrCountMap: collection.Map[String, Int],
                          criticalCookieMap: collection.Map[String, Int], mintimeMap: collection.Map[String, Int],
                          aCriticalPagesAccTimeMap: collection.Map[String, Int],
                          values: ArrayBuffer[FlowCollocation]): AntiCalculateResult = {

            //获取这个ip对应在八个结果集中的值
            //截取ip的第一个.
            val first = ip.indexOf(".")
            //截取ip的第2个.
            val secondary = ip.indexOf(".", first + 1)
            //截取ip段
            val ipblock = ip.substring(0, secondary)
            ////////////////////////////////////////////////////
            //获取这个ip段对应的值
            val ipBlockCounts = ipblockMap.getOrElse(ipblock, 0)
            //获取这个ip访问的总量
            val ipCounts = ipCountMap.getOrElse(ip, 0)
            //这条记录对应的单位时间内的关键页面访问总量
            val criticalPageAccessCounts = criticalPagesCountMap.getOrElse(ip, 0)
            //这条记录对应的单位时间内的 UA 种类数统计
            val userAgentCounts = userAgentsMap.getOrElse(ip, 0)
            //这条记录对应的单位时间内的关键页面最短访问间隔
            val critivalPageMinInterval = mintimeMap.getOrElse(ip, 0)
            //这条记录对应的单位时间内小于最短访问间隔（自设）的关键页面查询次数
            val accessPageIntervalLessThanDefault = aCriticalPagesAccTimeMap.getOrElse(ip, 0)
            //这条记录对应的单位时间内查询不同行程的次数
            val differentTripQuerysCounts = depArrCountMap.getOrElse(ip, 0)
            //这条记录对应的单位时间内关键页面的 Cookie 数
            val criticalCookies = criticalCookieMap.getOrElse(ip, 0)

            //封装八个结果集到Map
            val Maps = Map(
              "ipBlock" -> ipBlockCounts,
              "criticalPages" ->criticalPageAccessCounts,
              "userAgent"->userAgentCounts,
              "criticalPagesAccTime"->critivalPageMinInterval,
              "criticalCookies"->criticalCookies,
              "flightQuery"->differentTripQuerysCounts,
              "criticalPagesLessThanDefault"->accessPageIntervalLessThanDefault,
              "ip"->ipCounts
            )
          // 为实际打分准备数据
          //values是流程数据
          val flowsScore: Array[FlowScoreResult]=calculateFlowsScore(Maps,values)

    //返回最终结果
    AntiCalculateResult(processeddata,ip,ipBlockCounts,ipCounts,criticalPageAccessCounts,userAgentCounts,critivalPageMinInterval,
      accessPageIntervalLessThanDefault,differentTripQuerysCounts,criticalCookies,flowsScore: Array[FlowScoreResult])
  }




  // 为实际打分准备数据
  //values是流程数据
  def calculateFlowsScore(Maps: Map[String, Int], values: ArrayBuffer[FlowCollocation]): Array[FlowScoreResult] = {
          val  flowsScore= new ArrayBuffer[FlowScoreResult]

        //初始化 不管选没选择，只要数据对应的值超过系统设置的值，就获取对应的分数（权重），没超过就赋值0
        //这个结果集要是8个，
        val all=new ArrayBuffer[Double]

        //  选择了的，并且数据对应的值超过系统设置的值，获取对应的分数（权重）
        //这个结果集内数据的数量不定，有几个超出的指标就有几个指标对应的分数（权重）
        val select=new ArrayBuffer[Double]

        //命中规则的list
        val hitRules =new ArrayBuffer[String]

        //遍历每个流程  value 每个流程
        for(value<-values){
          //获取这个流程内的8个规则
            val rules: List[RuleCollocation]=value.rules
              //循环8个规则，  rule一规则
              for(rule<-rules){
                    //获取系统内配置的每个规则默认配置的阈值
                    val systemValue=  if(rule.ruleName.equalsIgnoreCase("criticalPagesLessThanDefault"))rule.ruleValue1 else rule.ruleValue0
                    //获取数据内每个指标对应的量
                    val dataValue=Maps.getOrElse(rule.ruleName,0)
                    //进行对比  如果数据的值大于系统设置的值  添加
                    if(dataValue>systemValue){
                      //添加分数
                      all.append(rule.ruleScore)
                      //添加命中的规则ID
                      hitRules.append(rule.ruleId)

                      //添加必选规则   当状态是选择的时候，添加到select
                      if(rule.ruleStatus==0){
                        select.append(rule.ruleScore)
                      }

                    }else{
                      all.append(0)
                    }
              }

          //调用核心打分算法  返回最终得分   score就是这一条数据最终的得分
          val score=computeScore(all.toArray,select.toArray)
         if ( score>0){
           println(">>>>>>-------------->>  "+score)
         }
          //数据得分是否大于系统配置的最终阈值
          val isUpLimited: Boolean=score>value.flowLimitScore
          if (isUpLimited){
            println(">>>>>>==============>>  "+isUpLimited.toString)
          }

          flowsScore.append(FlowScoreResult(value.flowId,score,value.flowLimitScore,isUpLimited,value.strategyCode, hitRules.toList,new Date().toString))
        }


    flowsScore.toArray
  }


  /**
    *  通过算法计算打分
    *  系数 2 权重：60%，数据区间：10-60
    *  系数 3 权重：40，数据区间：0-40
    *  系数 2+系数 3 区间为：10-100
    *  系数 1 为:平均分/10
    *  所以，factor1 * (factor2 + factor3)区间为:平均分--10 倍平均分
    * @return
    */
  def computeScore(scores: Array[Double],xa: Array[Double]): Double = {
    //打分列表
    //总打分
    val sum = scores.sum
    //打分列表长度
    val dim = scores.length
    //系数 1：平均分/10
    val factor1 = sum / (10 * dim)
    //命中数据库开放规则的 score
    //命中规则中，规则分数最高的
    val maxInXa = if (xa.isEmpty) {
      0.0
    } else {
      xa.max
    }
    //系数 2：系数 2 的权重是 60，指的是最高 score 以 6 为分界，最高 score 大于 6，就给
    //满权重 60，不足 6，就给对应的 maxInXa*10
    val factor2 = if (1 < (1.0 / 6.0) * maxInXa) {
      60
    } else {
      (1.0 / 6.0) * maxInXa * 60
    }
    //系数 3：打开的规则总分占总规则总分的百分比，并且系数 3 的权重是 40
    val factor3 = 40 * (xa.sum / sum)
    /**
      *  系数 2 权重：60%，数据区间：10-60
      *  系数 3 权重：40，数据区间：0-40
      *  系数 2+系数 3 区间为：10-100
      *  系数 1 为:平均分/10
      *  所以，factor1 * (factor2 + factor3)区间为:平均分--10 倍平均分
      */
    factor1 * (factor2 + factor3)
  }
}
