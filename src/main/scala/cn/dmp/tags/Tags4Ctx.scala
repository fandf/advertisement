package cn.dmp.tags

import cn.dmp.utils.TagsUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Tags4Ctx extends App {

  //1.接收程序参数
  //输入路径
  val logInputPath = "C:\\Users\\feng\\Desktop\\广告数据\\输出"
  //字典文件路径
  val dictFilePath = "C:\\Users\\feng\\Desktop\\广告数据\\app_mapping.txt"
  //停用词库
  val stopWordsFilePath = "C:\\Users\\feng\\Desktop\\广告数据\\stopwords.txt"
  //输出路径
  val resultOutputPath = "C:\\Users\\feng\\Desktop\\广告数据\\Tags4Ctx"

  //2.创建sparkConf=>sparkContext
  val sparkConf = new SparkConf()
  sparkConf.setAppName(s"${this.getClass.getSimpleName}")
  sparkConf.setMaster("local[*]")
  //RDD 序列化到磁盘 worker与worker之间的数据传输
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  //app字典
  val dictMap: Map[String, String] = sc.textFile(dictFilePath).map(line => {
    val fields = line.split("\t", -1)
    (fields(4), fields(1))
  }).collect().toMap
  val broadcast = sc.broadcast(dictMap)


  //停用字典
  val stopWordsMap = sc.textFile(stopWordsFilePath).map((_, 0)).collect().toMap
  val broadcastStopWords = sc.broadcast(stopWordsMap)

  sqlContext.read.parquet(logInputPath).where(TagsUtils.hasSomeUserIdCondition)
    .map(row => {
    //行数据进行标签化处理
    val ads = Tags4Ads.makeTags(row)
    val apps = Tags4App.makeTags(row, broadcast.value)
    val devices = TagsDevices.makeTags(row)
    val keyWords = Tags4KeyWords.makeTags(row, broadcastStopWords.value)
    val allUserId = TagsUtils.getAllUserId(row)
    (allUserId(0), (ads ++ apps ++ devices ++ keyWords).toList)
  }).reduceByKey((a, b) => {
    (a ++ b).groupBy(_._1).map{
      case (k, sameTags) => (k, sameTags.map(_._2).sum)
    }.toList
  }).saveAsTextFile(resultOutputPath)


  sc.stop()


}

