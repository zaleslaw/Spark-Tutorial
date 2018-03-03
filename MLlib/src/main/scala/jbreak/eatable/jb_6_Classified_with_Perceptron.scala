package jbreak.eatable

import jbreak.eatable.jb_4_Classified_with_SVM.enrichPredictions
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Neural network works well too with such type of architecture.
  * Also it makes feature reduction from 16 to 2
  *
  * Accuracy is comparable with DT, but FP, FN errors are better for our life quality
  */
object jb_6_Classified_with_Perceptron {
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
      .option("charset", "windows-1251")
      .option("header", "true")
      .csv("/home/zaleslaw/data/cyr_binarized_animals.csv")

    // from 2-dimension space to 16-dimension space improves the prediction
    val assembler = new VectorAssembler()
      .setInputCols(Array("hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "toothed", "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"))
      .setOutputCol("features")

    // Step - 2: Transform dataframe to vectorized dataframe
    val output = assembler.transform(animals).select("features", "eatable", "cyr_name")

    output.cache()

    // specify layers for the neural network:
    // input layer of size 16 (features), intermediate of size 15,14,13,12,11,10
    // and output of size 2 (classes)
    val layers = Array[Int](16, 15, 14, 13, 12, 11, 10, 2)

    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setSeed(1234L)
      .setMaxIter(100)
      .setLabelCol("eatable")
      .setFeaturesCol("features")

    val model = trainer.fit(output)

    val rawPredictions = model.transform(output)

    val predictions: DataFrame = enrichPredictions(spark, rawPredictions)

    predictions.show(100, false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("eatable")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(rawPredictions)
    println("Test Error = " + (1.0 - accuracy))

  }
}
