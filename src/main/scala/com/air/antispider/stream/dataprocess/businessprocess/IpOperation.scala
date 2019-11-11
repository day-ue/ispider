package com.air.antispider.stream.dataprocess.businessprocess

import scala.collection.mutable.ArrayBuffer
/*
是否是高频ip的判断
 */
object IpOperation {
  //判断
  def isFreIP(remoteAddr: String, values: ArrayBuffer[String]):Boolean = {

    //初始化的Boolean
    var flag=false
    //遍历历史的黑名单IP
    for(value<-values){
      //若当前的ip与黑名单的IP相等，表示这个ip是历史出现的黑名单IP
      if (remoteAddr.equals(value)){
        flag=true
      }
    }
    flag
  }

}
