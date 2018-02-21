package jbreak.classes

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Try to find clusters in dataset 'animals' and compare it with real classes
  */
object Ex_1_Clustering_with_KMeans {
    def main(args: Array[String]): Unit = {

        //For windows only: don't forget to put winutils.exe to c:/bin folder
        System.setProperty("hadoop.home.dir", "c:\\")

        val spark = SparkSession.builder
            .master("local")
            .appName("Spark_SQL")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val animals = readAnimalDataset(spark)

        // Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler
        val assembler = new VectorAssembler()
            .setInputCols(Array("hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "predator", "toothed", "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"))
            .setOutputCol("features")

        // Step - 2: Transform dataframe to vectorized dataframe
        val vectorizedDF = assembler.transform(animals).select("features", "cyr_name", "Cyr_Class_Type")

        // Step - 3: Train model
        val kmeans = new KMeans()
            .setK(7) // possible number of clusters: change it from 2 to 10, optimize parameter
            .setSeed(1L)
            .setFeaturesCol("features")
            .setPredictionCol("cluster")

        val model = kmeans.fit(vectorizedDF)

        // Step - 4: Print out the sum of squared distances of points to their nearest center
        val SSE = model.computeCost(vectorizedDF)
        println(s"Sum of Squared Errors = $SSE")

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
            .select("cyr_name", "cluster")
            .groupBy("cluster")
            .agg(collect_list("cyr_name")).show(predictions.count().toInt, false)

        println("Real classes")

        vectorizedDF
            .select("cyr_name", "Cyr_Class_Type")
            .groupBy("Cyr_Class_Type")
            .agg(collect_list("cyr_name")).show(predictions.count().toInt, false)
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
