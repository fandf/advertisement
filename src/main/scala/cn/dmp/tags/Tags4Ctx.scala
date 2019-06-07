package cn.dmp.tags

import java.text.SimpleDateFormat
import java.util.Date

import cn.dmp.utils.TagsUtils
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
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

  //判断hbase表是否存在，不存在则创建
  val load = ConfigFactory.load()
  val hbTableName = load.getString("hbase.table.name")
  val configuration: Configuration = sc.hadoopConfiguration
  configuration.set("hbase.zookeeper.quorum", load.getString("hbase.zookeeper.host"))
  val hbConn: Connection = ConnectionFactory.createConnection(configuration)
  val hbaseAdmin = hbConn.getAdmin
  val day = TagsUtils.getNowDate()

  if(!hbaseAdmin.tableExists(TableName.valueOf(hbTableName)) ){
    println(s"$hbTableName 不存在。。。")
    println(s"正在创建  $hbTableName ")
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbTableName))
    val columnDescriptor = new HColumnDescriptor("cf")
    tableDescriptor.addFamily(columnDescriptor)
    hbaseAdmin.createTable(tableDescriptor)
    //释放连接
    hbaseAdmin.close()
    hbConn.close()
  }


  //指定key输出的类型
  val jobConf = new JobConf(configuration)
  jobConf.setOutputFormat(classOf[TableOutputFormat])
  //表名称
  jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbTableName)

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
  }).map{
    case (userId, userTags) => {
      val put = new Put(Bytes.toBytes(userId))
      val tags = userTags.map(t => t._1 + ":" + t._2).mkString(",")
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(day), Bytes.toBytes(tags))
      (new ImmutableBytesWritable(), put)
    }
  }.saveAsHadoopDataset(jobConf)
//    .saveAsTextFile(resultOutputPath)


  sc.stop()


}

