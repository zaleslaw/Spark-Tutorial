package jbreak.classes

import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel, OneVsRest}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
  * Try to find clusters in small dataset and compare it with real classes
  */
object Ex_3_Multi_Classified_with_SVM {
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


        /*    val assembler = new VectorAssembler()
                .setInputCols(Array("legs","tail"))
                .setOutputCol("features")*/

        val assembler = new VectorAssembler()
            .setInputCols(Array("hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "toothed", "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"))
            .setOutputCol("features")

        // Step - 2: Transform dataframe to vectorized dataframe
        val output = assembler.transform(animals).select("features", "name", "type")

        // Step - 3: Train model
        val classifier = new LinearSVC()
            .setMaxIter(20)
            .setRegParam(0.8)
            .setLabelCol("type")

        // instantiate the One Vs Rest Classifier.
        val multiClassTrainer = new OneVsRest().setClassifier(classifier).setLabelCol("type")

        // train the multiclass model.
        val model = multiClassTrainer.fit(output)

        model.models
            .map(e => e.asInstanceOf[LinearSVCModel])
            .foreach(
                mdl => println(s"Coefficients for specific model : ${mdl.coefficients} and intercept: ${mdl.intercept}"))

        val predictions = model.transform(output)
        predictions.show(100, true)

        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("type")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println("Test Error = " + (1.0 - accuracy))

    }
}
