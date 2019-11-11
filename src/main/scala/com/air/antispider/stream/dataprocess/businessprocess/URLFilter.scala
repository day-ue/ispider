package com.air.antispider.stream.dataprocess.businessprocess

import scala.collection.mutable.ArrayBuffer

/*
进行数据过滤的代码
 */
object URLFilter {
  //数据过滤的造作方法
  def filterURL(message: String, values: ArrayBuffer[String]): Boolean = {
    //默认全部保留
    var save : Boolean = true
    //默认的url
    var URL = ""
    //如果元数据使用“#CS#”分割后的长度大于1，那么继续
    if (message.split("#CS#", -1).length > 1) {
      //截取出第2个值（URL）
      URL = message.split("#CS#", -1)(1)
    }
    //循环遍历每一个过滤规则
    for (value <- values) {
      //若url能匹配上规则，标识是需要删除掉的数据
      if (URL.matches(value)) {
        //将是否需要保留的值改成false
        save = false
      }
    }
    save
  }
}
