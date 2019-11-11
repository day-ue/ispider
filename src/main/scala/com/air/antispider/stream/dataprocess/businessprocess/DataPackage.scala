package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean._
import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum.TravelTypeEnum

object DataPackage {
  def dataPackage(sourceData: String, requestMethod: String, request: String, remoteAddr: String, httpUserAgent: String, timeIso8601: String,
                  serverAddr: String, highFrqIPGroup: Boolean, requestType: RequestType, travelType: TravelTypeEnum,
                  cookieValue_JSESSIONID: String, cookieValue_USERID: String, queryRequestData: Option[QueryRequestData],
                  queryBookRequestData: Option[BookRequestData], httpReferrer: String): ProcessedData = {
    //初始化一个ProcessedData
    val processedData: ProcessedData=null
    //flightDate: String, depcity: String, arrcity: String
/*
    var flightDate=""
    //现在queryRequestData内部进行判断获取
    queryRequestData match {
      case Some(query)=>  flightDate=query.flightDate
      case None=>
    }
    //现在queryBookRequestData内部进行判断获取
    if (flightDate==""){
      queryBookRequestData  match {
        case Some(book)=>  flightDate=book.flightDate.toString()
        case None=>
      }
    }


    var depcity=""
    //现在queryRequestData内部进行判断获取
    queryRequestData match {
      case Some(query)=>  depcity=query.depCity
      case None=>
    }
    //现在queryBookRequestData内部进行判断获取
    if (depcity==""){
      queryBookRequestData  match {
        case Some(book)=>  depcity=book.depCity.toString()
        case None=>
      }
    }



    var arrcity=""
    //现在queryRequestData内部进行判断获取
    queryRequestData match {
      case Some(query)=>  arrcity=query.arrCity
      case None=>
    }
    //现在queryBookRequestData内部进行判断获取
    if (arrcity==""){
      queryBookRequestData  match {
        case Some(book)=>  arrcity=book.arrCity.toString()
        case None=>
      }
    }
*/


    ///////////////////////////////////////
   var flightDate=""
    var depcity1=""
    var arrcity1=""
    //现在queryRequestData内部进行判断获取
    queryRequestData match {
      case Some(query)=>
        flightDate=query.flightDate
        depcity1=query.depCity
        arrcity1=query.arrCity
      case None=>
    }
    //现在queryBookRequestData内部进行判断获取
    if (flightDate==""){
      queryBookRequestData  match {
        case Some(book)=>
          flightDate=book.flightDate.toString()
          depcity1=book.depCity.toString()
          arrcity1=book.arrCity.toString()
        case None=>
      }
    }

    ///////////////////////////////////////
      //封装核心请求信息      飞行时间、出发地、目的地
      val  requestParams= CoreRequestParams(flightDate, depcity1, arrcity1)

    ProcessedData("", requestMethod, request,remoteAddr, httpUserAgent, timeIso8601,serverAddr, highFrqIPGroup,
      requestType, travelType,requestParams,cookieValue_JSESSIONID, cookieValue_USERID,queryRequestData, queryBookRequestData,httpReferrer)



  }

}
