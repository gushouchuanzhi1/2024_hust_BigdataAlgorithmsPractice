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
      val filePath = "hdfs://localhost:9000/user/hadoop/data/Cit-HepPh.txt"
      
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
      }.repartition(sc.defaultParallelism) 
      .mapPartitions { partition =>
        import org.apache.hadoop.io.compress.SnappyCodec
        val codec = new SnappyCodec
        val compressor = codec.createOutputStream(new ByteArrayOutputStream())
        partition.map(edge => Edge(edge.srcId, edge.dstId, edge.attr, compressor))
      }
      .cache()

      val graph = Graph.fromEdges(edges, 1).partitionBy(PartitionStrategy.EdgePartition2D)

      sc.setLocalProperty("spark.scheduler.pool", "pool1")

      val triangleCounts = graph.triangleCount().vertices

      val writer = new PrintWriter(new File("result.txt"))
      writer.write(s"Total number of triangles in the graph: ${triangleCounts.collect().map(_._2).sum / 3}\n")

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
