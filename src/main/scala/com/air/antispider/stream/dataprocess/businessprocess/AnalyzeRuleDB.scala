package com.air.antispider.stream.dataprocess.businessprocess

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util

import com.air.antispider.stream.common.bean.{AnalyzeRule, FlowCollocation, RuleCollocation}
import com.air.antispider.stream.common.util.database.{QueryDB, c3p0Util}
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, FlightTypeEnum}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/*
读取数据库规则
 */
object AnalyzeRuleDB {

  /**
    *  获取流程列表
    *  参数 n 为 0 为反爬虫流程
    *参数 n 为 1 为防占座流程
    *
    * @return ArrayBuffer[FlowCollocation]
    */
  def createFlow(n:Int) :ArrayBuffer[FlowCollocation] = {
    var array = new ArrayBuffer[FlowCollocation]
    var sql:String = ""
    if(n  ==  0){
      sql  =  "select nh_process_info.id,nh_process_info.process_name,nh_strategy.crawler_blacklist_thresholds from" +
        " nh_process_info,nh_strategy where nh_process_info.id=nh_strategy.id and status=0"}
      else  if(n  ==  1){
          sql  =  "select nh_process_info.id,nh_process_info.process_name,nh_strategy.occ_blacklist_thresholds  from nh_process_info,nh_strategy where nh_process_info.id=nh_strategy.id and status=1"}
        var conn: Connection = null
        var ps: PreparedStatement = null
        var rs:ResultSet = null
        try{
          conn = c3p0Util.getConnection
          ps = conn.prepareStatement(sql)
          rs = ps.executeQuery()
          while (rs.next()) {
            val flowId = rs.getString("id")
            val flowName = rs.getString("process_name")
            if(n == 0){
              val flowLimitScore = rs.getDouble("crawler_blacklist_thresholds")
              array  +=  new  FlowCollocation(flowId,  flowName,createRuleList(flowId,n), flowLimitScore, flowId)
            }else if(n == 1){
              val flowLimitScore = rs.getDouble("occ_blacklist_thresholds")
              array  +=  new  FlowCollocation(flowId,  flowName,createRuleList(flowId,n),
                flowLimitScore, flowId)
            }
          }
        }catch{
          case e : Exception => e.printStackTrace()
        }finally {
          c3p0Util.close(conn, ps, rs)
        }
        array
      }

  /**
    *  获取规则列表
    *
    * @param process_id  根据该 ID 查询规则
    * @return list 列表
    */
  def createRuleList(process_id:String,n:Int):List[RuleCollocation] = {
    var list = new ListBuffer[RuleCollocation]
    val  sql  =  "select  *  from(" +
      "select nh_rule.id,nh_rule.process_id,nh_rules_maintenance_table.rule_real_name,nh_rule.rule_type,nh_rule.crawler_type,"+
    "nh_rule.status,nh_rule.arg0,nh_rule.arg1,nh_rule.score  from nh_rule,nh_rules_maintenance_table where nh_rules_maintenance_table."+
    "rule_name=nh_rule.rule_name" +
      ")  as  tab  where  process_id  =  '"+process_id  +  "'and crawler_type="+n
    //and status="+n
    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs:ResultSet = null
    try{
      conn = c3p0Util.getConnection
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()
      while ( rs.next() ) {
        val ruleId = rs.getString("id")
        val flowId = rs.getString("process_id")
        val ruleName = rs.getString("rule_real_name")
        val ruleType = rs.getString("rule_type")
        val ruleStatus = rs.getInt("status")
        val ruleCrawlerType = rs.getInt("crawler_type")
        val ruleValue0 = rs.getDouble("arg0")
        val ruleValue1 = rs.getDouble("arg1")
        val ruleScore = rs.getInt("score")
        val  ruleCollocation  =  new
            RuleCollocation(ruleId,flowId,ruleName,ruleType,ruleStatus,ruleCrawlerType,ruleValue0,ruleValue1,ruleScore)
        list += ruleCollocation
      }
    }catch {
      case e : Exception => e.printStackTrace()
    }finally {
      c3p0Util.close(conn, ps, rs)
    }
    list.toList
  }
////////////////////////////////////////////////////////////









  //关键页面的规则
  def queryCriticalPages(): ArrayBuffer[String] ={
    val  queryCriticalPagesSql  =  "select  criticalPageMatchExpression  from  nh_query_critical_pages"
      val queryCriticalPagesField = "criticalPageMatchExpression"
      val queryCriticalPages = QueryDB.queryData(queryCriticalPagesSql, queryCriticalPagesField)
      queryCriticalPages
      }

  /*
读取黑名单-高频 IP 的数据（nh_ip_blacklist 表）
*/
  def getBlackIpDB():   ArrayBuffer[String]   ={
    //数据查询的语句
    val sql ="select ip_name from   nh_ip_blacklist"
    //接数据字段
    val field="ip_name"
    //       获取数据
    val biackip = QueryDB.queryData(sql,field)
    biackip
  }


  /*
  读取数据库中的数据过滤规则
   */
  def queryFilterRule(): ArrayBuffer[String] = {
    //规则的查询，在数据库中查询出来，并返回
    val queryFilterRuleSQL = "select value from nh_filter_rule"
    val value = "value"
    val filterRule = QueryDB.queryData(queryFilterRuleSQL, value)
    filterRule
  }


  /*
  读取飞行类型，操作类型的规则
   */
  //数据分类  查询
  //飞行类型（0-国内，1-国际）     操作类型（0-查询，1-预定）
  //    0                                       0     （国内查询）
  //     0                                       1    （国内预定）
  //    1                                        0     （国际查询）
  //    1                                       1      （国际预定）

  def queryRuleMap(): util.HashMap[String, ArrayBuffer[String]] = {
    //读取数据库的规则
    //国内查询
    val nationalQuerySQL: String = "select expression from nh_classify_rule where flight_type=" + FlightTypeEnum.National.id + " and operation_type=" + BehaviorTypeEnum.Query.id
    //国际查询
    val InternationalQuerySQL: String = "select expression from nh_classify_rule where flight_type=" + FlightTypeEnum.International.id  + " and operation_type=" + BehaviorTypeEnum.Query.id
    //国内预定
    val nationalBookSQL: String = "select expression from nh_classify_rule where flight_type=" + FlightTypeEnum.National.id + " and operation_type=1" + BehaviorTypeEnum.Book.id
    //国际预定
    val InternationalBookSQL: String = "select expression from nh_classify_rule where flight_type=" + FlightTypeEnum.International.id  + " and operation_type=1" + BehaviorTypeEnum.Book.id

    val field: String = "expression"

    //读取国内查询正则表达式
    val nationalQueryExpression: ArrayBuffer[String] = QueryDB.queryData(nationalQuerySQL, field)
    //读取 国际查询正则表达式
    val InternationalQueryExpression: ArrayBuffer[String] = QueryDB.queryData(InternationalQuerySQL, field)
    //读取国内预定正则表达式
    val nationalBookExpression: ArrayBuffer[String] = QueryDB.queryData(nationalBookSQL, field)
    //读取国际预定正则表达式
    val InternationalBookExpression: ArrayBuffer[String] = QueryDB.queryData(InternationalBookSQL, field)

    //将读取出来的正则表达式封装到一个map
    val expressionMap=new java.util.HashMap[String, ArrayBuffer[String]]
    expressionMap.put("nationalQueryExpression",nationalQueryExpression)
    expressionMap.put("InternationalQueryExpression",InternationalQueryExpression)
    expressionMap.put("nationalBookExpression",nationalBookExpression)
    expressionMap.put("InternationalBookExpression",InternationalBookExpression)
    expressionMap
  }




  /**
    *  解析规则
    *  查询"查询规则"或者“预定规则”正则表达式，添加到广播变量
    *
    * @return
    */
  def queryRule(behaviorType: Int): List[AnalyzeRule] = {
    //mysql 中解析规则（0-查询，1-预订）数据
    var analyzeRuleList = new ArrayBuffer[AnalyzeRule]()
    val sql: String = "select * from analyzerule where behavior_type =" + behaviorType
    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null
    try {
      conn = c3p0Util.getConnection
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()
      while (rs.next()) {
        val analyzeRule = new AnalyzeRule()
        analyzeRule.id = rs.getString("id")
        analyzeRule.flightType = rs.getString("flight_type").toInt
        analyzeRule.BehaviorType = rs.getString("behavior_type").toInt
        analyzeRule.requestMatchExpression = rs.getString("requestMatchExpression")
        analyzeRule.requestMethod = rs.getString("requestMethod")
        analyzeRule.isNormalGet = rs.getString("isNormalGet").toBoolean
        analyzeRule.isNormalForm = rs.getString("isNormalForm").toBoolean
        analyzeRule.isApplicationJson = rs.getString("isApplicationJson").toBoolean
        analyzeRule.isTextXml = rs.getString("isTextXml").toBoolean
        analyzeRule.isJson = rs.getString("isJson").toBoolean
        analyzeRule.isXML = rs.getString("isXML").toBoolean
        analyzeRule.formDataField = rs.getString("formDataField")
        analyzeRule.book_bookUserId = rs.getString("book_bookUserId")
        analyzeRule.book_bookUnUserId = rs.getString("book_bookUnUserId")
        analyzeRule.book_psgName = rs.getString("book_psgName")
        analyzeRule.book_psgType = rs.getString("book_psgType")
        analyzeRule.book_idType = rs.getString("book_idType")
        analyzeRule.book_idCard = rs.getString("book_idCard")
        analyzeRule.book_contractName = rs.getString("book_contractName")
        analyzeRule.book_contractPhone = rs.getString("book_contractPhone")
        analyzeRule.book_depCity = rs.getString("book_depCity")
        analyzeRule.book_arrCity = rs.getString("book_arrCity")
        analyzeRule.book_flightDate = rs.getString("book_flightDate")
        analyzeRule.book_cabin = rs.getString("book_cabin")
        analyzeRule.book_flightNo = rs.getString("book_flightNo")
        analyzeRule.query_depCity = rs.getString("query_depCity")
        analyzeRule.query_arrCity = rs.getString("query_arrCity")
        analyzeRule.query_flightDate = rs.getString("query_flightDate")
        analyzeRule.query_adultNum = rs.getString("query_adultNum")
        analyzeRule.query_childNum = rs.getString("query_childNum")
        analyzeRule.query_infantNum = rs.getString("query_infantNum")
        analyzeRule.query_country = rs.getString("query_country")
        analyzeRule.query_travelType = rs.getString("query_travelType")
        analyzeRule.book_psgFirName = rs.getString("book_psgFirName")
        analyzeRuleList += analyzeRule
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      c3p0Util.close(conn, ps, rs)
    }
    analyzeRuleList.toList
  }


}
