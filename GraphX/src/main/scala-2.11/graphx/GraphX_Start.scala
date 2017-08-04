package graphx


import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object GraphX_Start {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .master("local[2]")
      .appName("GraphX Start")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext

    // Create an RDD for the vertices
    val vertices: RDD[(VertexId, (Int, Int))] =
      sc.parallelize(Array((1L, (7, -1)), (2L, (3, -1)),
        (3L, (2, -1)), (4L, (6, -1))))

    // Create an RDD for edges
    val relationships: RDD[Edge[Boolean]] =
      sc.parallelize(Array(Edge(1L, 2L, true), Edge(1L, 4L, true),
        Edge(2L, 4L, true), Edge(3L, 1L, true),
        Edge(3L, 4L, true)))

    // Create the graph
    val graph = Graph(vertices, relationships)

    // Check the graph
    graph.vertices.collect.foreach(println)

  }
}

