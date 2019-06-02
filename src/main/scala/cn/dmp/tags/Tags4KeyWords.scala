package cn.dmp.tags

import org.apache.spark.sql.Row

object Tags4KeyWords extends Tags {
  /**
    * 打标签的方法定义
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {

    var map = Map[String, Int]()
    val row = args(0).asInstanceOf[Row]
    var stopWords = args(1).asInstanceOf[Map[String, Int]]
    //关键字
    val kws = row.getAs[String]("keywords")
    kws.split("\\|")
      .filter(kw => kw.trim.length >= 3 && kw.trim.length <= 8 && !stopWords.contains(kw))
      .foreach(map += "K" + _ -> 1)

    map

  }
}
