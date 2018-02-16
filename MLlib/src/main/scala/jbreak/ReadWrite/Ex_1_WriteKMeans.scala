package jbreak.ReadWrite

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
  * How to share trained model in Spark?
  * The best approach - is serialization and sharing through the file.
  */
object Ex_1_WriteKMeans {
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
        println(model.summary.clusterSizes.toString)
        model.write.overwrite.save("KMeans")

    }
}
