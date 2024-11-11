import scala.collection.mutable
import scala.io.Source
import System.nanoTime

object LouvainAlgorithm {

  case class Graph(edges: mutable.Map[Int, mutable.Map[Int, Double]], nodes: mutable.Set[Int]) {
    def addEdge(from: Int, to: Int, weight: Double): Unit = {
      if (!edges.contains(from)) edges(from) = mutable.Map()
      if (!edges.contains(to)) edges(to) = mutable.Map()

      edges(from)(to) = weight
      edges(to)(from) = weight
    }

    def getEdgeWeight(from: Int, to: Int): Double = {
      edges.getOrElse(from, mutable.Map()).getOrElse(to, 1.0) // Default weight 1 for unweighted graphs
    }

    def size(): Double = {
      edges.values.flatMap(_.values).sum / 2
    }

    def degree(node: Int): Double = {
      edges.getOrElse(node, mutable.Map()).values.sum
    }

    def neighbors(node: Int): Set[Int] = {
      edges.getOrElse(node, mutable.Map()).keys.toSet
    }
  }

  // 计算模块度
  def calculateModularity(graph: Graph, communities: Map[Int, Set[Int]]): Double = {
    val m = graph.size()
    var Q = 0.0

    for ((community, nodes) <- communities) {
      val internalWeight = nodes.flatMap(u => nodes.map(v => if (u != v) graph.getEdgeWeight(u, v) else 0.0)).sum
      val degreeSum = nodes.map(graph.degree).sum
      Q += internalWeight / (2 * m) - Math.pow(degreeSum / (2 * m), 2)
    }

    Q
  }

  // 初始化每个节点为一个独立的社区
  def initializeCommunities(graph: Graph): Map[Int, Set[Int]] = {
    graph.nodes.map(node => node -> Set(node)).toMap
  }

  // 计算模块度增益
  def modularityGain(graph: Graph, node: Int, community: Set[Int], communityWeight: Double, totalWeight: Double): Double = {
    val nodeDegree = graph.degree(node)
    val communityDegreeSum = community.map(graph.degree).sum
    val linksToCommunity = community.map(n => graph.getEdgeWeight(node, n)).sum
    (linksToCommunity - (nodeDegree * communityDegreeSum) / (2 * totalWeight)) / (2 * totalWeight)
  }

  // Louvain算法
  def louvainAlgorithm(graph: Graph, targetCommunities: Int): Map[Int, Set[Int]] = {
    var communities = initializeCommunities(graph)
    var nodeToCommunity = graph.nodes.map(node => node -> node).toMap

    var improved = true
    while (improved) {
      improved = false

      // 节点遍历，计算增益并移动到最佳社区
      for (node <- graph.nodes) {
        val originalCommunity = nodeToCommunity(node)
        var bestCommunity = originalCommunity
        var bestIncrease = 0.0

        communities = communities.updated(originalCommunity, communities(originalCommunity) - node)

        // 获取相邻社区
        val neighborCommunities = graph.neighbors(node).map(nodeToCommunity).toSet

        for (community <- neighborCommunities) {
          val increase = modularityGain(graph, node, communities(community), graph.size(), graph.size())
          if (increase > bestIncrease) {
            bestIncrease = increase
            bestCommunity = community
          }
        }

        if (bestCommunity != originalCommunity) {
          nodeToCommunity = nodeToCommunity.updated(node, bestCommunity)
          communities = communities.updated(bestCommunity, communities(bestCommunity) + node)
          improved = true
        } else {
          communities = communities.updated(originalCommunity, communities(originalCommunity) + node)
        }
      }

      // 如果没有改进或达到目标社区数，结束
      if (!improved || communities.size <= targetCommunities) {
        break
      }

      // 重建图，并进行下一轮的社区发现
      var newGraph = new Graph(mutable.Map(), mutable.Set())
      var newCommunities = mutable.Map[Int, Set[Int]]()
      for ((community, nodes) <- communities) {
        val newNode = community
        newGraph.nodes += newNode
        newCommunities(newNode) = nodes
        for (node <- nodes) {
          for (neighbor <- graph.neighbors(node)) {
            val neighborCommunity = nodeToCommunity(neighbor)
            val weight = graph.getEdgeWeight(node, neighbor)
            newGraph.addEdge(newNode, neighborCommunity, weight)
          }
        }
      }

      graph = newGraph
      communities = newCommunities.toMap
      nodeToCommunity = communities.flatMap { case (community, nodes) => nodes.map(node => node -> community) }
    }

    communities
  }

  def main(args: Array[String]): Unit = {
    // 初始化图
    val graph = new Graph(mutable.Map(), mutable.Set())

    // 读取文件并加载图结构
    for (line <- Source.fromFile("Cit-HepPh.txt").getLines()) {
      if (!line.startsWith("#")) {
        val Array(fromNode, toNode) = line.split("\\s+").map(_.toInt)
        graph.addEdge(fromNode, toNode, 1.0)
      }
    }

    // 获取图的信息
    println(s"图的节点数: ${graph.nodes.size}")
    println(s"图的边数: ${graph.edges.size}")

    // 记录程序开始时间
    val startTime = nanoTime()

    // 应用 Louvain 算法进行社区划分，指定目标社区数为 5
    val communities = louvainAlgorithm(graph, targetCommunities = 5)

    // 记录程序结束时间
    val endTime = nanoTime()

    // 计算程序运行时间
    val executionTime = (endTime - startTime) / 1e9
    println(f"程序运行时间: $executionTime%.2f 秒")

    // 输出结果
    println("最终社区划分：")
    for ((community, nodes) <- communities) {
      println(s"社区 $community: 包含节点 ${nodes.mkString(", ")}")
    }

    // 将结果写入文件
    val resultContent = new StringBuilder
    resultContent.append(s"图的节点数: ${graph.nodes.size}\n")
    resultContent.append(s"图的边数: ${graph.edges.size}\n")
    resultContent.append("最终社区划分：\n")
    for ((community, nodes) <- communities) {
      resultContent.append(s"社区 $community: 包含节点 ${nodes.mkString(", ")}\n")
    }
    resultContent.append(f"程序运行时间: $executionTime%.2f 秒\n")

    import java.io._
    val pw = new PrintWriter(new File("code1ans.txt"))
    pw.write(resultContent.toString)
    pw.close()

    println("结果已保存到 code1ans.txt 文件中")
  }
}
