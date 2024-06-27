import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._

object TriangleCountingApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TriangleCounting").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val startTime = System.currentTimeMillis()

    try {
      // 使用 HDFS 路径
      val filePath = "hdfs://localhost:9000/user/hadoop/data/Cit-HepPh.txt"
      
      // 读取文件并处理成无向边，并进行数据分区和缓存，并使用数据压缩
      val edges: RDD[Edge[Int]] = sc.textFile(filePath).flatMap { line =>
        val fields = line.split("\\s+")
        if (fields.length == 2) {
          val src = fields(0).toLong
          val dst = fields(1).toLong
          if (src != dst) {
            List(Edge(math.min(src, dst), math.max(src, dst), 1))
          } else {
            List.empty
          }
        } else {
          List.empty
        }
      }.repartition(sc.defaultParallelism) // 根据默认并行度进行数据分区
      .mapPartitions { partition =>
        import org.apache.hadoop.io.compress.SnappyCodec
        val codec = new SnappyCodec
        val compressor = codec.createOutputStream(new ByteArrayOutputStream())
        partition.map(edge => Edge(edge.srcId, edge.dstId, edge.attr, compressor))
      }
      .cache() // 缓存 RDD

      // 构建图并对顶点进行分区
      val graph = Graph.fromEdges(edges, 1).partitionBy(PartitionStrategy.EdgePartition2D)

      // 调整并行度和任务分配策略
      sc.setLocalProperty("spark.scheduler.pool", "pool1")

      // 计算三角形数量
      val triangleCounts = graph.triangleCount().vertices

      // 将结果写入本地文件系统
      val writer = new PrintWriter(new File("result.txt"))
      writer.write(s"Total number of triangles in the graph: ${triangleCounts.collect().map(_._2).sum / 3}\n")

      // 写入每个顶点参与的三角形数量
      triangleCounts.collect().foreach { case (vertexId, count) =>
        writer.write(s"Vertex $vertexId is part of $count triangles.\n")
      }

      writer.close()

    } finally {
      val endTime = System.currentTimeMillis()
      val totalTime = endTime - startTime
      println(s"Program took $totalTime milliseconds to run.")
      sc.stop()
    }
  }
}
