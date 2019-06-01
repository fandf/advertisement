package cn.dmp.report

import cn.dmp.beans.Log
import cn.dmp.utils.{JedisPools, RptUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

object AppAnalyseRptRedis extends App {

  //1.接收程序参数
  val logInputPath = "C:\\Users\\feng\\Desktop\\广告数据\\输入\\2016-10-01_06_p1_invalid.1475274123982.log.FINISH.bz2"
  val resultOutputPath = "C:\\Users\\feng\\Desktop\\广告数据\\appAnalyseRedis"


  //2.创建sparkConf=>sparkContext
  val sparkConf = new SparkConf()
  sparkConf.setAppName(s"${this.getClass.getSimpleName}")
  sparkConf.setMaster("local[*]")
  //RDD 序列化到磁盘 worker与worker之间的数据传输
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)


  sc.textFile(logInputPath)
    .map(_.split(",", -1))
    .filter(_.length >= 85)
    .map(Log(_)).filter(log => !log.appid.isEmpty || !log.appname.isEmpty)
    .mapPartitions(itr => {
      val jedis = JedisPools.getJedis()
      val parResult = new collection.mutable.ListBuffer[(String, List[Double])]()
      itr.foreach(log => {
        var newAppname = log.appname
        if (StringUtils.isBlank(newAppname)) {
          newAppname = jedis.get(log.appid)
        }
        val reqList = RptUtils.caculateReq(log.requestmode, log.processnode)
        val rtbList = RptUtils.caculateRtb(log.iseffective, log.isbilling, log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
        parResult += ((newAppname, reqList ++ rtbList))
      })
      jedis.close()
      parResult.toIterator
    }).reduceByKey((list1, list2) => {
    list1.zip(list2).map(t => t._1 + t._2)
  }).map(t => t._1 + "," + t._2.mkString(","))
    .saveAsTextFile(resultOutputPath)


  sc.stop()

}
