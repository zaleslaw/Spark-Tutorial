package jbreak.eatable

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
  * Try to find clusters in small dataset and compare it with real classes
  */
object Ex_3_Classified_with_Perceptron {
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
            .csv("/home/zaleslaw/data/binarized_animals.csv")

        // from 2-dimension space to 16-dimension space improves the prediction
        val assembler = new VectorAssembler()
            .setInputCols(Array("hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "toothed", "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"))
            .setOutputCol("features")

        // Step - 2: Transform dataframe to vectorized dataframe
        val output = assembler.transform(animals).select("features", "name", "type", "eatable")

        // specify layers for the neural network:
        // input layer of size 4 (features), two intermediate of size 5 and 4
        // and output of size 3 (classes)
        val layers = Array[Int](16, 15, 14, 13, 12, 11, 10, 2)

        // create the trainer and set its parameters
        val trainer = new MultilayerPerceptronClassifier()
            .setLayers(layers)
            .setSeed(1234L)
            .setMaxIter(100)
            .setLabelCol("eatable")
            .setFeaturesCol("features")

        val model = trainer.fit(output)

        val predictions = model.transform(output)

        predictions.show(100, false)

        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("eatable")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println("Test Error = " + (1.0 - accuracy))

    }
}
