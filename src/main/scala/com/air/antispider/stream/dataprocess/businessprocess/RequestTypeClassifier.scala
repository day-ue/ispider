package com.air.antispider.stream.dataprocess.businessprocess

import java.util

import com.air.antispider.stream.common.bean.RequestType
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, FlightTypeEnum}

import scala.collection.mutable.ArrayBuffer

/*
分析 数据飞行标签
 */
object RequestTypeClassifier {

  //匹配数据
  def classifyByRequest(request:String,values:util.HashMap[String, ArrayBuffer[String]]): RequestType ={

    var requestType:RequestType=null
    var flag=true

    //获取国内查询的正则
    val nationalQueryExpressions: ArrayBuffer[String]=values.get("nationalQueryExpression")
    //获取国内预定的正则
    val nationalBookExpressions: ArrayBuffer[String]=values.get("nationalBookExpression")
    //获取国际查询的正则
    val InternationalQueryExpressions: ArrayBuffer[String]=values.get("InternationalQueryExpression")
    //获取国际预定的正则
    val InternationalBookExpressions: ArrayBuffer[String]=values.get("InternationalBookExpression")

    //用数据匹配国内查询的正则
    for(nationalQueryExpression<-nationalQueryExpressions if flag){
      //进行匹配
      if(request.matches(nationalQueryExpression)){
        requestType=RequestType(FlightTypeEnum.National,BehaviorTypeEnum.Query)
        flag=false
      }
    }

    //用数据匹配国内预定的正则
    for(nationalBookExpression<-nationalBookExpressions if flag){
      if(request.matches(nationalBookExpression)){
        requestType=RequestType(FlightTypeEnum.National,BehaviorTypeEnum.Book)
        flag=false
      }
    }

    //用数据匹配国际查询的正则
    for(internationalQueryExpression<-InternationalQueryExpressions if flag){
      if (request.matches(internationalQueryExpression)){
        requestType=RequestType(FlightTypeEnum.International,BehaviorTypeEnum.Query)
        flag=false
      }
    }

    //用数据匹配国际预定的正则
    for(internationalBookExpression<-InternationalBookExpressions if flag){
      if (request.matches(internationalBookExpression)){
        requestType=RequestType(FlightTypeEnum.International,BehaviorTypeEnum.Book)
        flag=false
      }
    }

    if (flag){
      requestType=RequestType(FlightTypeEnum.Other,BehaviorTypeEnum.Other)
    }
    requestType
  }

}
