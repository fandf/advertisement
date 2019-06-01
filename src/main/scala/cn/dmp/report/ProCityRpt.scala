package cn.dmp.report

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计日志文件中省市的数据量分布情况
  *
  * 需求1：
  *   将统计出来的数据存储成json格式
  * 需求2：
  *   将统计结果存储到mysql中
  */
object ProCityRpt {

  def main(args: Array[String]): Unit = {

    //0.校验参数个数
    if(args.length != 2){
      println(
        """
          |cn.dmp.tools.Bzip2Parquet
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

    //判断存储路径是否存在，在则删除
    val hadoopConfiguration = sc.hadoopConfiguration
    val fs = FileSystem.get(hadoopConfiguration)

    val path = new Path(resultOutputPath)
    if(fs.exists(path)){
      fs.delete(path, true)
    }


    //存储成json文件
    result.coalesce(1).write.json(resultOutputPath)

    sc.stop()

  }

}
