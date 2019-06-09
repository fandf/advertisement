package cn.dmp.tags

import cn.dmp.utils.{JedisPools, TagsUtils}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Tags4CtxV2 extends App {

  //1.接收程序参数
  //输入路径
  val logInputPath = "C:\\Users\\feng\\Desktop\\广告数据\\输出"
  //字典文件路径
  val dictFilePath = "C:\\Users\\feng\\Desktop\\广告数据\\app_mapping.txt"
  //停用词库
  val stopWordsFilePath = "C:\\Users\\feng\\Desktop\\广告数据\\stopwords.txt"
  //输出路径
  val resultOutputPath = "C:\\Users\\feng\\Desktop\\广告数据\\Tags4CtxV2"

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

  val baseData: DataFrame = sqlContext.read.parquet(logInputPath).where(TagsUtils.hasSomeUserIdCondition)

  val uv: RDD[(Long, (ListBuffer[String], List[(String, Int)]))] = baseData.mapPartitions(par => {
    val jedis = JedisPools.getJedis()
    val listBuffer = new collection.mutable.ListBuffer[(Long, (ListBuffer[String], List[(String, Int)]))]()
    par.foreach(row => {
      //行数据进行标签化处理
      val ads = Tags4Ads.makeTags(row)
      val apps = Tags4App.makeTags(row, broadcast.value)
      val devices = TagsDevices.makeTags(row)
      val keyWords = Tags4KeyWords.makeTags(row, broadcastStopWords.value)

      val allUserId = TagsUtils.getAllUserId(row)
      val tags = (ads ++ apps ++ devices ++ keyWords).toList
      //点的集合数据
      listBuffer.append((allUserId(0).hashCode.toLong, (allUserId, tags)))
      listBuffer
    })
    jedis.close()
    listBuffer.toIterator
  })

  //构建边集合
  val ue: RDD[Edge[Int]] = baseData.flatMap(row => {
    val allUserId = TagsUtils.getAllUserId(row)
    allUserId.map(uId => Edge(allUserId(0).hashCode.toLong, uId.hashCode.toLong, 0))
  })
  val graph = Graph(uv, ue)
  val cc = graph.connectedComponents().vertices

  cc.join(uv).map({
    case (xxId, (cmId, (uids, tags))) => (cmId, (uids, tags))
  }).reduceByKey({
    case (a, b) => {
      val uIds = a._1 ++ b._1
      val tags = ((a._2 ++ b._2).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2))).toList
      (uIds.distinct, tags)
    }
  }).saveAsTextFile(resultOutputPath)


  sc.stop()


}

