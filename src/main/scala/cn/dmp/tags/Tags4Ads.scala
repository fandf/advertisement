package cn.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object Tags4Ads extends Tags {
  /**
    * 打标签的方法定义
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String, Int]()
    val row = args(0).asInstanceOf[Row]
    //广告位类型和名称
    val adTypeId = row.getAs[Int]("adspacetype")
    val adTypeName = row.getAs[String]("adspacetypename")

    if(adTypeId > 9) map += "LC"+adTypeId -> 1
    else if(adTypeId > 0) map += "LC" + adTypeId -> 1

    if(StringUtils.isNotBlank(adTypeName)) map += "LN" + adTypeName -> 1

    //渠道
    val channelId = row.getAs[Int]("adplatformproviderid")
    if(channelId > 0) map += (("CN"+channelId, 1))

    map
  }


}
