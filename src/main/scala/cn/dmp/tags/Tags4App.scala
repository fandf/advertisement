package cn.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object Tags4App extends Tags {
  /**
    * 打标签的方法定义
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String, Int]()
    val row = args(0).asInstanceOf[Row]
    val appDict = args(1).asInstanceOf[Map[String, String]]

    //广告位类型和名称
    val appId = row.getAs[String]("appid")
    val appName = row.getAs[String]("appname")

    if(StringUtils.isBlank(appName)) {
      appDict.contains(appId) match {
        case true => map += "APP"+appDict.get(appId) -> 1
      }
    }


    map
  }


}
