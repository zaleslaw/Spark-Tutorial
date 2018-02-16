package jbreak.ReadWrite

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
  * How to read the model to reuse in Spark code?
  */
object Ex_2_ReadKMeans {
    def main(args: Array[String]): Unit = {

        //For windows only: don't forget to put winutils.exe to c:/bin folder
        System.setProperty("hadoop.home.dir", "c:\\")

        val spark = SparkSession.builder
            .master("local")
            .appName("Spark_SQL")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val animals = spark.read
            .option("inferSchema", "true")
            .option("header", "true")
            .csv("/home/zaleslaw/data/animals.csv")

        animals.show()
        val assembler = new VectorAssembler()
            .setInputCols(Array("hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "predator", "toothed",
                "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"))
            .setOutputCol("features")

        // Step - 2: Transform dataframe to vectorized dataframe
        val output = assembler.transform(animals).select("features", "name", "type")

        val model = KMeansModel.load("KMeans")

        // Step - 4: Print out the
        // sum of squared distances of points to their nearest center
        val SSE = model.computeCost(output)
        println(s"Sum of Squared Errors = $SSE")

        // Step - 5: Print out clusters
        println("Cluster Centers: ")
        model.clusterCenters.foreach(println)

        println("Real clusters and predicted clusters")
        val predictions = model.transform(output)
        predictions.show(200)



        // Step - 6: Print out predicted and real classes
        import org.apache.spark.sql.functions._

        println("Predicted classes")
        predictions
            .select("name", "cluster")
            .groupBy("cluster")
            .agg(collect_list("name")).show(predictions.count().toInt, false)

        println("Real classes")
        output
            .select("name", "type")
            .groupBy("type")
            .agg(collect_list("name")).show(predictions.count().toInt, false)
    }
}
