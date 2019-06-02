package cn.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsDevices extends Tags {
  /**
    * 打标签的方法定义
    * 设备
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {

    var map = Map[String, Int]()
    val row = args(0).asInstanceOf[Row]

    val os = row.getAs[Int]("client")
    val phoneType = row.getAs[String]("device")
    val ntm = row.getAs[String]("networkmannername")
    val ispname = row.getAs[String]("ispname")

    os match {
      case 1 => map += "D00010001" -> 1
      case 2 => map += "D00010002" -> 1
      case 3 => map += "D00010003" -> 1
      case _ => map += "D00010004" -> 1
    }

    if (StringUtils.isNotBlank(phoneType)) map += "DN"+phoneType -> 1

    ntm.toUpperCase match {
      case "WIFI" => map += "D00020001" -> 1
      case "4G" => map += "D00020002" -> 1
      case "3G" => map += "D00020003" -> 1
      case "2G" => map += "D00020004" -> 1
      case _ => map += "D00020005" -> 1
    }

    ispname match {
      case "移动" => map += "D00030001" -> 1
      case "联通" => map += "D00030002" -> 1
      case "电信" => map += "D00030003" -> 1
      case _ => map += "D00030004" -> 1
    }

    val pName = row.getAs[String]("provincename")
    val cName = row.getAs[String]("cityname")
    if (StringUtils.isNotBlank(pName)) map += "ZP"+pName -> 1
    if (StringUtils.isNotBlank(cName)) map += "ZC"+cName -> 1

    map

  }
}
