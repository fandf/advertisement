package cn.dmp.report

import cn.dmp.beans.Log
import cn.dmp.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 广告投放的地域分布统计
  * 实现方式： spark core
  */
object AreaAnalyseRptRDD {

  def main(args: Array[String]): Unit = {
    //0.校验参数个数
    if(args.length != 2){
      println(
        """
          |cn.dmp.report.AreaAnalyseRptRDD
          |参数：
          | logInputPath
          | resultOutputPath
        """.stripMargin
      )
      sys.exit()
    }
    //1.接收程序参数
    val Array(logInputPath, resultOutputPath) = args

    //2.创建sparkConf=>sparkContext
    val sparkConf = new  SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    //RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)

    val data = sc.textFile(logInputPath)
      .map(_.split(",", -1))
      .filter(_.length >= 85)
      .map(arr => {
        val log = Log(arr)
        val reqList = RptUtils.caculateReq(log.requestmode, log.processnode)
        val rtbList = RptUtils.caculateRtb(log.iseffective, log.isbilling, log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
        ((log.provincename, log.cityname), reqList ++ rtbList)
      }).reduceByKey((list1, list2) => {
        list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => t._1._1 + "," + t._1._2 +","+t._2.mkString(","))
      .saveAsTextFile(resultOutputPath)



    sc.stop()
  }

}
