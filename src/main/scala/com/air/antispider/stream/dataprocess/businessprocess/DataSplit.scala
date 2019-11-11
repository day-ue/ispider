package com.air.antispider.stream.dataprocess.businessprocess

import java.util.regex.Pattern
import com.air.antispider.stream.common.util.decode.{RequestDecoder, EscapeToolBox}
import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import com.air.antispider .stream.common.util.string.CsairStringUtils
/*
进行数据切分的代码
 */
object DataSplit {
  //数据切分
  def dataSplit(encryptedID: String):(String,String,String,String,String,String,String,String,String,String,String,String ) = {
    //使用“#CS#”对数据进行切分
    val values: Array[String] = encryptedID.split("#CS#", -1)
    //获得切分后的length
    val valuesLength: Int = values.length

    //临时的request 包含 POST  request  HTTP
     var tmprRequest = ""
    //有效的request
    var request :String = ""
    //现获取临时的request
    if (encryptedID.split("#CS#", -1).length > 1) {
      tmprRequest = encryptedID.split("#CS#", -1)(1)
    }
    //获取最终的request
    if (tmprRequest.split(" ", -1).length > 1) {
      request = tmprRequest.split(" ", -1)(1)
    }

    //请求方式 GET/POST
    val requestMethod:String  = if (valuesLength > 2) values(2) else ""
    //content_type
    val contentType :String = if (valuesLength > 3) values(3) else ""
    //Post 提交的数据体
    val requestBody :String = if (valuesLength > 4) values(4) else ""
    //http_referrer
    val httpReferrer :String = if (valuesLength > 5) values(5) else ""
    //客户端 IP
    val remoteAddr:String  = if (valuesLength > 6) values(6) else ""
    //客户端 UA
    val httpUserAgent:String  = if (valuesLength > 7) values(7) else ""
    //服务器时间的 ISO8610 格式
    val timeIso8601 :String = if (valuesLength > 8) values(8) else ""
    //服务器地址
    val serverAddr :String = if (valuesLength > 9) values(9) else ""
    //Cookie 信息
    //原始信息中获取 Cookie 字符串，去掉空格，制表符
    val cookiesStr = CsairStringUtils.trimSpacesChars(if (valuesLength > 10) values(10) else "")
    //提取 Cookie 信息并保存为 K-V 形式
    val cookieMap = {
      var tempMap = new scala.collection.mutable.HashMap[String, String]
      if (!cookiesStr.equals("")) {
        cookiesStr.split(";").foreach { s =>
          val kv = s.split("=")
          //UTF8 解码
          if (kv.length > 1) {
            try {
              val chPattern = Pattern.compile("u([0-9a-fA-F]{4})")
              val chMatcher = chPattern.matcher(kv(1))
              var isUnicode = false
              while (chMatcher.find()) {
                isUnicode = true
              }
              if (isUnicode) {
                tempMap += (kv(0) -> EscapeToolBox.unescape(kv(1)))
              } else {
                tempMap += (kv(0) -> RequestDecoder.decodePostRequest(kv(1)))
              }
            } catch {
              case e: Exception => e.printStackTrace()
            }
          }
        }
      }
      tempMap
    }
    //Cookie 关键信息解析
    //从配置文件读取 Cookie 配置信息
    val  cookieKey_JSESSIONID:String   =  PropertiesUtil.getStringByKey("cookie.JSESSIONID.key",
      "cookieConfig.properties")
    val  cookieKey_userId4logCookie:String   =  PropertiesUtil.getStringByKey("cookie.userId.key",
      "cookieConfig.properties")
    //Cookie-JSESSIONID
    val cookieValue_JSESSIONID:String  = cookieMap.getOrElse(cookieKey_JSESSIONID, "NULL")
    //Cookie-USERID-用户 ID
    val cookieValue_USERID :String = cookieMap.getOrElse(cookieKey_userId4logCookie, "NULL")
    (request,requestMethod,contentType,requestBody,httpReferrer,remoteAddr,httpUserAgent,timeIso8601,serverAddr,cookiesStr,cookieValue_JSESSIONID,cookieValue_USERID)
  }
}


