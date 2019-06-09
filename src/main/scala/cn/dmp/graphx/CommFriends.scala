package cn.dmp.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CommFriends extends App {


  //2.创建sparkConf=>sparkContext
  val sparkConf = new SparkConf()
  sparkConf.setAppName(s"${this.getClass.getSimpleName}")
  sparkConf.setMaster("local[1]")
  //RDD 序列化到磁盘 worker与worker之间的数据传输
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)

  //构建点
  val uv: RDD[(VertexId, (String, Int))] = sc.parallelize(Seq(
    (1, ("和川", 23)),
    (2, ("苑十文", 24)),
    (6, ("常婷", 18)),
    (9, ("杨洋", 24)),
    (133, ("王新", 30)),

    (16, ("杨俊", 23)),
    (21, ("刘瑞川", 25)),
    (44, ("崔斌", 20)),
    (138, ("张贤", 31)),

    (5, ("高景涛", 28)),
    (7, ("陈芳", 19)),
    (158, ("张宪一", 26))


  ))

  //构建边
  val ue: RDD[Edge[Int]] = sc.parallelize(Seq(
    Edge(1, 133, 0),
    Edge(2, 133, 0),
    Edge(9, 133, 0),
    Edge(6, 133, 0),

    Edge(6, 138, 0),
    Edge(16, 138, 0),
    Edge(44, 138, 0),
    Edge(21, 138, 0),

    Edge(5, 158, 0),
    Edge(7, 158, 0)
  ))

  val graph = Graph(uv, ue)

  val commonV: VertexRDD[VertexId] = graph.connectedComponents().vertices
//  commonV.map(t => (t._2, List(t._1))).reduceByKey( _ ++ _).foreach(println)
  /**
    * uv --> (userId, (姓名， 年龄))
    * commonV --> (userId, 共同的顶点)
    */
  uv.join(commonV).map{
    case (userId, ((name, age), cmId)) => (cmId, List((name, age)))
  }.reduceByKey(_ ++ _).foreach(println)


  sc.stop()

}
