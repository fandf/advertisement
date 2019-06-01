package cn.dmp.report

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计日志文件中省市的数据量分布情况
  *
  * 需求1：
  *   将统计出来的数据存储成json格式
  * 需求2：
  *   将统计结果存储到mysql中
  */
object ProCityRptMysql {

  def main(args: Array[String]): Unit = {

    //0.校验参数个数
    if(args.length != 2){
      println(
        """
          |cn.dmp.tools.ProCityRptMysql
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

    //读取数据
    val sqlc = new SQLContext(sc)
    val df: DataFrame = sqlc.read.parquet(logInputPath)

    df.registerTempTable("log")
    //按照省市进行分组聚合
    val result: DataFrame = sqlc.sql("select provincename, cityname, count(*) ct from log group by provincename, cityname")


    val load = ConfigFactory.load()
    val properties = new Properties()
    properties.setProperty("user", load.getString("jdbc.user"))
    properties.setProperty("password", load.getString("jdbc.password"))

    result.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"), load.getString("jdbc.tableName"), properties)

    sc.stop()

  }

}
