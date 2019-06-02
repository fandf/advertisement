package cn.dmp.report

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 广告投放的地域分布统计
  * 实现方式： sparksql
  */
object AreaAnalyseRpt {

  def main(args: Array[String]): Unit = {
    //0.校验参数个数
    val logInputPath = "C:\\Users\\feng\\Desktop\\广告数据\\输出"
    val resultOutputPath = "C:\\Users\\feng\\Desktop\\广告数据\\appAnalyseRedis"
    //2.创建sparkConf=>sparkContext
    val sparkConf = new  SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    //RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)

    val sqlc = new SQLContext(sc)
    val df: DataFrame = sqlc.read.parquet(logInputPath)
    df.registerTempTable("log")
    val result: DataFrame = sqlc.sql(
      """
    select
      provincename,cityname,
      sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) s1,
      sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) s2,
      sum(case when iseffective = 1 and isbilling = 1 and isbid =1 and adorderid != 0 then 1 else 0 end) s3,
      sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then 1 else 0 end) s4
    from log
    group by provincename,cityname
      """.stripMargin)


    result.show()


    sc.stop()
  }

}
