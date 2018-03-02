package jbreak.eatable

import jbreak.eatable.Ex_1_Classified_with_SVM.enrichPredictions
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Dataset marked from the Data Scientist's point ov view.
  *
  * But the DT works well for this case.
  */
object Ex_2_Classified_with_Decision_Trees {
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

    /*    val assembler = new VectorAssembler()
            .setInputCols(Array("legs","tail"))
            .setOutputCol("features")*/

    // from 2-dimension space to 16-dimension space improves the prediction
    val assembler = new VectorAssembler()
      .setInputCols(Array("hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "toothed", "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"))
      .setOutputCol("features")

    // Step - 2: Transform dataframe to vectorized dataframe
    val output = assembler.transform(animals).select("features", "eatable", "cyr_name")

    val trainer = new DecisionTreeClassifier()
      .setLabelCol("eatable")
      .setFeaturesCol("features")

    val model = trainer.fit(output)

    val rawPredictions = model.transform(output.sample(false, 0.4))


    val predictions: DataFrame = enrichPredictions(spark, rawPredictions)

    predictions.show(100, false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("eatable")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(rawPredictions)
    println("Test Error = " + (1.0 - accuracy))

    val treeModel = model.asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)
  }
}
