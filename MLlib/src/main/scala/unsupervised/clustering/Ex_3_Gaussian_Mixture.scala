package unsupervised.clustering

import org.apache.spark.ml.clustering.{BisectingKMeans, GaussianMixture}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession


/**
  * Try to find clusters in small dataset and compare it with real classes
  */
object Ex_3_Gaussian_Mixture {
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

    // Step - 1: Make Vectors from dataframe columns using special Vector Assmebler

    val assembler = new VectorAssembler()
      .setInputCols(Array("hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "predator", "toothed",
        "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"))
      .setOutputCol("features")


    // Step - 2: Transform dataframe to vectorized dataframe
    val output = assembler.transform(animals).select("features", "name", "type")

    // Step - 3: Train model
    val kmeans = new GaussianMixture()
      .setK(7) // possible number of clusters: change it from 2 to 10, optimize parameter
      .setSeed(1L)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")
      .setProbabilityCol("prob")

    val model = kmeans.fit(output)

    // Step - 4: Print out the
    for (i <- 0 until model.getK) {
      println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
        s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
    }

    println("Real clusters and predicted clusters")
    val predictions = model.summary.predictions
    predictions.show(200, false)


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
