package cn.dmp.tags

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object Tags4Bussiness extends Tags {
  /**
    * 打标签的方法定义
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String, Int]()
    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]
    //广告位类型和名称
    val lat = row.getAs[String]("lat")
    val longs = row.getAs[String]("long")
    if(StringUtils.isNotBlank(lat) && StringUtils.isNotBlank(longs)){
//      lat > 3 and lat < 54 and long > 73 and long < 136
      val lat2 = lat.toDouble
      val lon2 = longs.toDouble
      if(lat2 > 3 && lat2 < 54 && lon2 > 73 && lon2 < 136){
        val geoHashCode = GeoHash.withCharacterPrecision(lat2, lon2, 8).toBase32
        val bussiness = jedis.get(geoHashCode)
          if(StringUtils.isNotBlank(bussiness)){
            bussiness.split(",").foreach(bs => map += "BS" +bs -> 1)
          }
      }
    }

    map

  }
}
