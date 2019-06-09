总结：
    1）Parquet文件存储(列式存储)
        a.可以跳过不符合条件的数据，只读取需要的列，能够获取更好的扫描性能
        b.压缩编码降低磁盘空间
        c.适配多种计算框架，查询引擎（Hive,pig等）；计算框架（MapReduce,Spark等），数据模型（Avro,Thrift等）

    2）spark算子应用
        map、flatMap、filter、case、groupBy、reduceByKey等

    3）地理位置GEOHASH算法
        GeoHash是一种将经纬度先进行二进制转换，然后使用0-9、b-z（去掉a,i,l,o）这32个字母进行base32编码成字符串的方法，
        并且使得在大部分情况下，字符串前缀匹配越多的距离越近。
        当GeoHash base32编码长度为8时，精度在19米左右，而当编码长度为9时，精度在2米左右，编码长度需要根据数据情况进行选择。

    4）用户画像流程
        标签体系，字典文件（redis）
        日志--》app_mapping.txt匹配打标签
        过滤stopwords.txt字典
        商圈标签：

    用了哪些框架，遇到什么问题？
    1）spark序列化问题

    2）类字段过多导致异常？
    （什么异常）spark1.6时，case class 字段不能超过22个
    extends Product with Serializable

    3)正确理解分布式程序
        driver和execute端代码性能问题

    4）数据倾斜，某个task数据量过大。
        1.过滤少数导致倾斜的key
        2.提高shuffle操作的并行度
        3.两阶段聚合（局部聚合+全局聚合）reduceByKey 先局部，在全局   groupByKey 全局聚合
        4.将reduce join 转为 map join
        5.采样倾斜的key，使用随机前缀

    软件版本
        hadoop2.8
        Hive1.2
        Zookeeper3.4
        Spark1.6(2.4)
        Hbase0.98x
        Scala 2.1.0
        spark es

    8.调优经验--spark
        spark官网，优化指南


    环境：
        集群：8台机器，每台40核，120内存
        数据量：每天有效数据200-300W，采用gzip压缩方式，每天总量在150-250G。

