package cn.dmp.report

import cn.dmp.beans.Log
import cn.dmp.utils.RptUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

object AppAnalyseRpt extends App {

  //1.接收程序参数
  val dictFilePath = "C:\\Users\\feng\\Desktop\\广告数据\\app_mapping.txt"
  val logInputPath = "C:\\Users\\feng\\Desktop\\广告数据\\输入\\2016-10-01_06_p1_invalid.1475274123982.log.FINISH.bz2"
  val resultOutputPath = "C:\\Users\\feng\\Desktop\\广告数据\\appAnalyse"


  //2.创建sparkConf=>sparkContext
  val sparkConf = new  SparkConf()
  sparkConf.setAppName(s"${this.getClass.getSimpleName}")
  sparkConf.setMaster("local[*]")
  //RDD 序列化到磁盘 worker与worker之间的数据传输
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)

  val dictMap: Map[String, String] = sc.textFile(dictFilePath).map(line => {
    val fields = line.split("\t", -1)
    (fields(4), fields(1))
  }).collect().toMap
  val broadcast = sc.broadcast(dictMap)

  sc.textFile(logInputPath)
    .map(_.split(",", -1))
    .filter(_.length >= 85)
    .map(Log(_)).filter(log => !log.appid.isEmpty || !log.appname.isEmpty)
    .map(log => {
      val newAppname = log.appname
      if(StringUtils.isBlank(newAppname)){
        broadcast.value.getOrElse(log.appid, "unknow")
      }
      val reqList = RptUtils.caculateReq(log.requestmode, log.processnode)
      val rtbList = RptUtils.caculateRtb(log.iseffective, log.isbilling, log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
      (newAppname, reqList ++ rtbList)
    }).reduceByKey((list1, list2) => {
    list1.zip(list2).map(t => t._1 + t._2)
  }).map(t => t._1 +","+t._2.mkString(","))
    .saveAsTextFile(resultOutputPath)


  sc.stop()

}
