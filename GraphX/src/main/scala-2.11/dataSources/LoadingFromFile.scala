package dataSources

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by zaleslaw on 04.08.17.
  */
object LoadingFromFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[2]")
      .appName("Loading from file")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext

    class VertexProperty()
    case class Point(val name: String) extends VertexProperty


    val invertedVertices = sc.textFile("/home/zaleslaw/data/London_tube_map.txt").flatMap { line => line.split("\\s+")
    }.distinct().zipWithUniqueId()

    invertedVertices.cache()
    invertedVertices.collect()
    invertedVertices.foreach(println)

    // we need to lookup in invertedVertices
    // Due to org.apache.spark.SparkException: This RDD lacks a SparkContext. It could happen in the following cases:
    // (1) RDD transformations and actions are NOT invoked by the driver, but inside of other transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid because t
    // we can't make inner loop with lookup for each pait

    // we best way is broadcasting variable, due to number of vertices is comparably small

    val bInvertedVertices = sc.broadcast(invertedVertices.collect().toMap)


    val edges: RDD[Edge[Long]] = sc.textFile("/home/zaleslaw/data/London_tube_map.txt")
      .map(line => line.split("\\s+"))
      .map(array => (bInvertedVertices.value.get(array(0)).get.toLong, bInvertedVertices.value.get(array(1)).get.toLong))
      .map(tuple => Edge(tuple._1, tuple._2, 1))


    val graph = Graph(invertedVertices.map(_.swap), edges, "Unknown station")


    println("num edges = " + graph.numEdges);
    println("num invertedVertices = " + graph.numVertices);

    // yet one way: transform initial file to other file with Vertex Ids and Yet one file with pairs (VertexId, Name)
  }

}
