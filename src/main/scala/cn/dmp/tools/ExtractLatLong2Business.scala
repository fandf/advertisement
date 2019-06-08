package cn.dmp.tools

import ch.hsr.geohash.GeoHash
import cn.dmp.utils.{BaiduGeoApi, JedisPools}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 用来抽取日志字段中的经纬度，并请求百度API，获取到商圈信息
  */
object ExtractLatLong2Business {

  def main(args: Array[String]): Unit = {

    //1.接收程序参数
    //输入路径
    val logInputPath = "C:\\Users\\feng\\Desktop\\广告数据\\输出"
    //字典文件路径
    val dictFilePath = "C:\\Users\\feng\\Desktop\\广告数据\\app_mapping.txt"

    //2.创建sparkConf=>sparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    //RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    sqlContext.read.parquet(logInputPath)
      .select("lat", "long")
      .where("lat > 3 and lat < 54 and long > 73 and long < 136")
      .distinct()
      .foreachPartition(itr => {
        val jedis = JedisPools.getJedis()
        itr.foreach(row => {
          val lat = row.getAs[String]("lat")
          val longs = row.getAs[String]("long")
          val geoHashCode = GeoHash.withCharacterPrecision(lat.toDouble, longs.toDouble, 8).toBase32
          val business = BaiduGeoApi.getBusiness(lat+","+longs)
          if(StringUtils.isNotBlank(business)){
            jedis.set(geoHashCode, business)
          }
        })
        jedis.close()
      })

    sc.stop()

  }

}
