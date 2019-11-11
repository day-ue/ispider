package com.air.antispider.stream.dataprocess.businessprocess

import java.util.regex.Pattern

import com.air.antispider.stream.common.util.decode.MD5

/*
推数据进行脱敏的方法
 */
object EncryptedData {
  //手机号的脱敏
  def encryptedPhone(message: String): String = {
    var tmpMessage:String=""
    //手机号正则
    val  phonePattern  = Pattern.compile("((13[0-9])|(14[5|7])|(15([0-3]|[5-9]))|(17[0-9])|(18[0,5-9]))\\d{8}")
    //匹配可能是手机号的数据 多个值
    val phones=phonePattern.matcher(message)
    val md5=new MD5()
    while(phones.find()){
      //拿到一个可能是手机号的值
        val phone:String=phones.group()
      //拿到可能是手机号的前一个位置的角标
      val befi: Int=message.indexOf(phone)-1
      //拿到可能是手机号的后一个位置的角标
      val afti: Int=befi+phone.length+1
      //拿到可能是手机号的前一个位置的数据
      val befs:String=message.charAt(befi).toString
      //拿到可能是手机号的后一个位置的数据
      val afts:String=message.charAt(afti).toString
      //前面不是数字的情况，继续，是数字那么一定不是手机号
      if (!befs.matches("^[0-9]$")){
        //若手机号是字符内的最后一位，肯定是手机号
        if(afti>message.length){
          //进行数据的替换
          tmpMessage= message.replace(phone,md5.getMD5ofStr(phone))
        }else{//若手机号不是字符内的最后一位， 继续分析

          //若手机号后一位的字符不是数字，肯定是手机号（前一位也不是数字）
          if(!afts.matches("^[0-9]$")){
            //进行数据的替换
            tmpMessage=  message.replace(phone,md5.getMD5ofStr(phone))
          }
        }

      }

    }
    tmpMessage
  }


  def encryptedId(encryptedPhone: String): String = {
    var tmpMessage:String=""
    //身份证号码正则
    val idPattern = Pattern.compile("(\\d{18})|(\\d{17}(\\d|X|x))|(\\d{15})")
    //匹配可能是身份证号的数据 多个值
    val ids=idPattern.matcher(encryptedPhone)
    val md5=new MD5()
    while(ids.find()){
      //拿到一个可能是身份证号的值
      val id:String=ids.group()
      //拿到可能是身份证号的前一个位置的角标
      val befi: Int=encryptedPhone.indexOf(id)-1
      //拿到可能是身份证号的后一个位置的角标
      val afti: Int=befi+id.length+1
      //拿到可能是身份证号的前一个位置的数据
      val befs:String=encryptedPhone.charAt(befi).toString
      //拿到可能是身份证号的后一个位置的数据
      val afts:String=encryptedPhone.charAt(afti).toString
      //前面不是数字的情况，继续，是数字那么一定不是身份证
      if (!befs.matches("^[0-9]$")){
        //若身份证 的是字符内的最后一位，肯定是身份证
        if(afti>encryptedPhone.length){
          //进行数据的替换
          tmpMessage= encryptedPhone.replace(id,md5.getMD5ofStr(id))
        }else{//若身份证号不是字符内的最后一位， 继续分析
          //若身份证号后一位的字符不是数字，肯定是身份证号（前一位也不是数字）
          if(!afts.matches("^[0-9]$")){
            //进行数据的替换
            tmpMessage=  encryptedPhone.replace(id,md5.getMD5ofStr(id))
          }
        }

      }

    }
    tmpMessage
  }


}
