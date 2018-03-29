package jbreak.define_classes

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Try to find clusters in dataset 'animals' and compare it with real classes
  */
object jb_1_Clustering_with_KMeans {
  def main(args: Array[String]): Unit = {

    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark_SQL")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Step - 0: Overview the animal dataset and read it
    val animals = readAnimalDataset(spark)

    animals.show

    // Step - 1: Make Vectors from dataframe's columns using special VectorAssembler object
    val assembler = new VectorAssembler()
      .setInputCols(Array("hair","milk", "eggs")) // + add "eggs"
      .setOutputCol("features")

    /*    val assembler = new VectorAssembler()
      .setInputCols(Array("hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "toothed", "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"))
      .setOutputCol("features")*/

    // Step - 2: Transform dataframe to vectorized dataframe
    val vectorizedDF = assembler.transform(animals).select("features", "cyr_name", "Cyr_Class_Type")
    vectorizedDF.cache()

    for (i <- 2 to 20) {

      println("Clusterize with " + i + " clusters")

      // Step - 3: Train model
      val kMeansTrainer = new KMeans()
        .setK(i) // possible number of clusters: change it from 2 to 10, optimize parameter
        .setSeed(1L)
        .setFeaturesCol("features")
        .setPredictionCol("cluster")

      val model = kMeansTrainer.fit(vectorizedDF)

      // Step - 4: Print out the sum of squared distances of points to their nearest center
      val SSE = model.computeCost(vectorizedDF)
      println(s"Sum of Squared Errors = $SSE")

      // Step - 5: Print out cluster centers
      println("Cluster Centers: ")
      model.clusterCenters.foreach(println)

      println("Real clusters and predicted clusters")
      val predictions = model.summary.predictions

      // Step - 6: Print out predicted and real classes
      import org.apache.spark.sql.functions._

      println("Predicted classes")

      predictions
        .select("cyr_name", "cluster")
        .groupBy("cluster")
        .agg(collect_list("cyr_name"))
        .orderBy("cluster")
        .show(predictions.count().toInt, false)

    }

    println("Real classes")
    vectorizedDF
      .select("cyr_name", "Cyr_Class_Type")
      .groupBy("Cyr_Class_Type")
      .agg(collect_list("cyr_name")).show(vectorizedDF.count().toInt, false)

  }

  def readAnimalDataset(spark: SparkSession): DataFrame = {
    val animals = spark.read
      .option("inferSchema", "true")
      .option("charset", "windows-1251")
      .option("header", "true")
      .csv("/home/zaleslaw/data/cyr_animals.csv")

    animals.show()

    val classNames = spark.read
      .option("inferSchema", "true")
      .option("charset", "windows-1251")
      .option("header", "true")
      .csv("/home/zaleslaw/data/cyr_class.csv")

    classNames.show(false)

    val animalsWithClassTypeNames = animals.join(classNames, animals.col("type").equalTo(classNames.col("Class_Number")))

    animalsWithClassTypeNames
  }
}
