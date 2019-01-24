package RW.ToIgnite

import jpoint.titanic.TitanicUtils
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
  * Predict surviving based on integer data
  * <p>
  * The main problem are nulls in data. Values to assemble (by VectorAssembler) cannot be null.
  */
object Ex_7_KMeans {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = TitanicUtils.getSparkSession

        val passengers = TitanicUtils.readPassengersWithCasting(spark)

        // Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler
        val assembler = new VectorAssembler()
            .setInputCols(Array("pclass", "sibsp", "parch", "age"))
            .setOutputCol("features")

        // Step - 2: Transform dataframe to vectorized dataframe with dropping rows
        val output = assembler.transform(
            passengers.na.drop(Array("pclass", "sibsp", "parch", "age")) // <============== drop row if it has nulls/NaNs in the next list of columns
        ).select("features", "survived")

        // Step - 3: Set up the Decision Tree Classifier
        val trainer =  new KMeans()
            .setK(2) // possible number of clusters: change it from 2 to 10, optimize parameter
            .setSeed(1L)
            .setFeaturesCol("features")
            .setPredictionCol("cluster")


        // Step - 4: Train the model
        val model = trainer.fit(output)
        model.write.overwrite().save("/home/zaleslaw/models/titanic/kmeans")

        // Step - 5: Predict with the model
        val rawPredictions = model.transform(output)

        // Step - 6: Print out the
        // sum of squared distances of points to their nearest center
        val WSSSE = model.computeCost(output)
        println(s"Within Set Sum of Squared Errors = $WSSSE")

        // Step - 7: Print out clusters
        println("Cluster Centers: ")
        model.clusterCenters.foreach(println)

        println("Real clusters and predicted clusters")
        val predictions = model.summary.predictions
        predictions.show(200)
    }
}
