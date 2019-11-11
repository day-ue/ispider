package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.ProcessedData
import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import com.air.antispider.stream.dataprocess.constants.BehaviorTypeEnum
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD

/*
进行数据推送，推送到kafka
 */
object DataSend {

  //查询业务数据的推送
  def sendQueryDataToKafka(dataPreProcess: RDD[ProcessedData]): Unit = {
    //过滤出只是查询的所有数据
   val datas= dataPreProcess.filter(processedData=>processedData.requestType.behaviorType.id==BehaviorTypeEnum.Query.id).map(x=>x.toKafkaString())
    //将数据写入kafka
    datas.foreach(println)
    //实例kafka producer需要的参数
    val props=new  java.util.HashMap[String,Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,PropertiesUtil.getStringByKey("default.brokers","kafkaConfig.properties"))
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,PropertiesUtil.getStringByKey("default.key_serializer_class_config","kafkaConfig.properties"))
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,PropertiesUtil.getStringByKey("default.value_serializer_class_config","kafkaConfig.properties"))
    props.put(ProducerConfig.BATCH_SIZE_CONFIG,PropertiesUtil.getStringByKey("default.batch_size_config","kafkaConfig.properties"))
    props.put(ProducerConfig.LINGER_MS_CONFIG,PropertiesUtil.getStringByKey("default.linger_ms_config","kafkaConfig.properties"))

    //读取查询数据将要写入的topic      target.query.topic
   val processedQueryTopic= PropertiesUtil.getStringByKey("target.query.topic","kafkaConfig.properties")

    //需要发送的数据
    datas.foreachPartition(partition=>{
      //kafka 生产者 producer
      val kafkaP=new KafkaProducer[String,String](props)

      partition.foreach(message=>{
        //数据的载体
        val record=new ProducerRecord[String,String](processedQueryTopic,message)
        //数据发送
        kafkaP.send(record)
      })
      //kafka关闭
      kafkaP.close()
    })


  }


  //预定数据的推送
  def sendBookDataToKafka(dataPreProcess: RDD[ProcessedData]): Unit = {
    //过滤出只是预定的所有数据
    val datas= dataPreProcess.filter(processedData=>processedData.requestType.behaviorType.id==BehaviorTypeEnum.Book.id).map(x=>x.toKafkaString())
    //将数据写入kafka
    datas.foreach(println)
    //实例kafka producer需要的参数
    val props=new  java.util.HashMap[String,Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,PropertiesUtil.getStringByKey("default.brokers","kafkaConfig.properties"))
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,PropertiesUtil.getStringByKey("default.key_serializer_class_config","kafkaConfig.properties"))
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,PropertiesUtil.getStringByKey("default.value_serializer_class_config","kafkaConfig.properties"))
    props.put(ProducerConfig.BATCH_SIZE_CONFIG,PropertiesUtil.getStringByKey("default.batch_size_config","kafkaConfig.properties"))
    props.put(ProducerConfig.LINGER_MS_CONFIG,PropertiesUtil.getStringByKey("default.linger_ms_config","kafkaConfig.properties"))

    //读取预定数据将要写入的topic      target.book.topic
    val processedQueryTopic= PropertiesUtil.getStringByKey("target.book.topic","kafkaConfig.properties")

    //需要发送的数据
    datas.foreachPartition(partition=>{
      //kafka 生产者 producer
      val kafkaP=new KafkaProducer[String,String](props)

      partition.foreach(message=>{
        //数据的载体
        val record=new ProducerRecord[String,String](processedQueryTopic,message)
        //数据发送
        kafkaP.send(record)
      })
      //kafka关闭
      kafkaP.close()
    })

  }


}
