package unsupervised.clustering

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
  * Try to find clusters in small dataset and compare it with real classes
  */
object Ex_1_KMeans {
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

    //Exception in thread "main" java.lang.IllegalArgumentException: Field "features" does not exist.
    // don't forget transform all your columns with VectorAssembler to 'features'

    // Step - 1: Make Vectors from dataframe columns using special Vector Assmebler

    val assembler = new VectorAssembler()
      .setInputCols(Array("hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "predator", "toothed",
        "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"))
      .setOutputCol("features")


    // Step - 2: Transform dataframe to vectorized dataframe
    val output = assembler.transform(animals).select("features", "name", "type")

    // Step - 3: Train model
    val kmeans = new KMeans()
      .setK(7) // possible number of clusters: change it from 2 to 10, optimize parameter
      .setSeed(1L)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")

    val model = kmeans.fit(output)

    // Step - 4: Print out the
    // sum of squared distances of points to their nearest center
    val WSSSE = model.computeCost(output)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Step - 5: Print out clusters
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    println("Real clusters and predicted clusters")
    val predictions = model.summary.predictions
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
