package dataSources

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * https://snap.stanford.edu/data/web-Google.html
  */
object LoadingWebGoogleContestGraph {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[2]")
      .appName("Loading web-Google")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext


    val textFile = sc.textFile("/home/zaleslaw/data/web-Google.txt")
    val arrayForm = textFile.filter(_.charAt(0) != '#').map(_.split("\\s+")).cache()

    val edges: RDD[Edge[String]] = arrayForm.
      map(line => Edge(line(0).toLong, line(1).toLong, ""))

    val graph = Graph.fromEdges(edges, -1)

    println("num edges = " + graph.numEdges);
    println("num vertices = " + graph.numVertices);
    println(graph.stronglyConnectedComponents(5))
  }

}
