package cn.dmp.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UserTagsMergeDemo {

  def main(args: Array[String]): Unit = {

    //1.接收程序参数
    //输入路径
    val logInputPath = "C:\\Users\\feng\\Desktop\\广告数据\\graph.txt"
    //字典文件路径
    val dictFilePath = "C:\\Users\\feng\\Desktop\\广告数据\\app_mapping.txt"
    //停用词库
    val stopWordsFilePath = "C:\\Users\\feng\\Desktop\\广告数据\\stopwords.txt"
    //输出路径
    val resultOutputPath = "C:\\Users\\feng\\Desktop\\广告数据\\Tags4Ctx"

    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[1]")
    //RDD 序列化到磁盘 worker与worker之间的数据传输
//    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val data: RDD[Array[String]] = sc.textFile(logInputPath).map(_.split("\t"))

    //构建点
    val uv: RDD[(VertexId, (String, List[(String,Int)]))] = data.flatMap(arr => {
      //区分人名和标签
      val userNames = arr.filter(_.indexOf(":") == -1)
//      val userTags = arr.filter(_.indexOf(":") != -1).toList
      val userTags = arr.filter(_.indexOf(":") != -1).map(kvs => {
        val kv = kvs.split(":")
        (kv(0), kv(1).toInt)
      }).toList

      userNames.map(name => {
        if(name.equals(userNames(0)))
          (name.hashCode.toLong, (name, userTags))
        else
          (name.hashCode.toLong, (name, List.empty))
      })
    })
    val ue = data.flatMap(arr => {
      val userNames = arr.filter(_.indexOf(":") == -1)
      userNames.map(name => Edge(userNames(0).hashCode.toLong, name.hashCode.toLong, 0))
    })
    //创建一个图
    val graph = Graph(uv, ue)
    val cc = graph.connectedComponents().vertices

    //聚合数据
    cc.join(uv).map({
      case(id, (cmId, (name, tags))) => (cmId,(Seq(name), tags))
    }).reduceByKey{
      case(t1, t2) => {
        val k = t1._1 ++ t2._1
        val s = t1._2 ++ t2._2 groupBy(_._1) mapValues(_.foldLeft(0)(_ + _._2)) toList
        val v = List.empty
        (k, s)
      }
    }
      .map(t => (t._2._1.toSet, t._2._2)).foreach(println)



    sc.stop()

  }

}
