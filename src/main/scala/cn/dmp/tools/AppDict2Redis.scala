package cn.dmp.tools

import cn.dmp.utils.JedisPools
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将app字典库存储到redis中
  */
object AppDict2Redis extends App {

  //1.接收程序参数
  val dictFilePath = "C:\\Users\\feng\\Desktop\\广告数据\\app_mapping.txt"


  //2.创建sparkConf=>sparkContext
  val sparkConf = new  SparkConf()
  sparkConf.setAppName(s"${this.getClass.getSimpleName}")
  sparkConf.setMaster("local[*]")
  //RDD 序列化到磁盘 worker与worker之间的数据传输
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)

  sc.textFile(dictFilePath).map(line => {
    val fields = line.split("\t", -1)
    (fields(4), fields(1))
  }).foreachPartition( itr =>{
    val jedis = JedisPools.getJedis()
    itr.foreach(t => jedis.set(t._1, t._2))
    jedis.close()
  })

}
