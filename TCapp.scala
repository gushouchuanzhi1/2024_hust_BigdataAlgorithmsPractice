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
      val edges: RDD[Edge[Int]] = sc.textFile(filePath).flatMap { line =>
        val fields = line.split("\\s+")
        if (fields.length == 2) {
          Some(Edge(fields(0).toLong, fields(1).toLong, 1))
        } else {
          None
        }
      }

      val graph = Graph.fromEdges(edges, 1)

      // 计算三角形数量
      val triangleCounts = graph.triangleCount().vertices

      // 计算图中所有三角形的总数
      val totalTriangles = triangleCounts.map { case (vertexId, count) =>
        count / 3
      }.reduce(_ + _)

      // 将结果写入本地文件系统
      val writer = new PrintWriter(new File("result.txt"))
      writer.write(s"Total number of triangles in the graph: $totalTriangles\n")

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
