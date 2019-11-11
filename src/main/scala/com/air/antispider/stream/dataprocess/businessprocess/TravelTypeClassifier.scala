package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum
import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum.TravelTypeEnum

object TravelTypeClassifier {
  def classifyByRefererAndRequestBody(httpReferrer: String): TravelTypeEnum = {
    var travelTypeEnum:TravelTypeEnum=null
    //是用来记录日期格式的数据出现的次数
    var count=0
    //日期正则
    val regex = "^(\\d{4})-(0\\d{1}|1[0-2])-(0\\d{1}|[12]\\d{1}|3[01])$"
    //使用？对数据进行拆分
    //httpReferrer------------->https://b2c.csair.com/B2C40/newTrips/static/main/page/booking/index.html?t=R&c1=CAN&c2=PEK&d1=2019-03-23&d2=2019-03-23&at=1&ct=0&it=0
    var tpmData1=""
    if (httpReferrer.contains("?") && httpReferrer.split("\\?").length>1){
      //截取出使用“？”进行截取的第2个值   tpmData1---------->t=R&c1=CAN&c2=PEK&d1=2019-03-23&d2=2019-03-23&at=1&ct=0
      tpmData1=httpReferrer.split("\\?")(1)
      //tmpData2---->[c1=CAN      d1=2019-03-23       d2=2019-03-23      at=1]
       var tmpData2: Array[String] = tpmData1.split("&")
      //tmpData----->  d1=2019-03-23或者c1=CAN
      for(tmpData<-tmpData2){
        //tmpDate---->  2019-03-23  或者  CAN
       val tmpDate = tmpData.split("=")(1)
        if(tmpDate.matches(regex)){
          count+=1
        }
      }
    }
    //一个都没有匹配到
    if(count==0){
      //返回Unknown
      travelTypeEnum=TravelTypeEnum.Unknown
    }else if(count==1){// 匹配到一个日期格式的数据  返回 OneWay
      travelTypeEnum=TravelTypeEnum.OneWay
    }else if(count==2){// 匹配到两个日期格式的数据  返回 RoundTrip
      travelTypeEnum=TravelTypeEnum.RoundTrip
    }
    travelTypeEnum
  }

}
