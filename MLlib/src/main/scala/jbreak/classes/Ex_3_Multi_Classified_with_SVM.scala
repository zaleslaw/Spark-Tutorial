package jbreak.classes

import jbreak.classes.Ex_2_Classified_with_Decision_Trees.enrichPredictions
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel, OneVsRest}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Let's see that SVM with One-vs-Rest approach works well for Multi-Class classification
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

        val (classNames, animals) = Ex_2_Classified_with_Decision_Trees.readAnimalsAndClassNames(spark)

        // Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler
        /*    val assembler = new VectorAssembler()
                .setInputCols(Array("legs","tail"))
                .setOutputCol("features")*/

        val assembler = new VectorAssembler()
            .setInputCols(Array("hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "toothed", "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"))
            .setOutputCol("features")

        // Step - 2: Transform dataframe to vectorized dataframe
        val output = assembler.transform(animals).select("features", "name", "type", "cyr_name", "Cyr_Class_Type")

        // Step - 3: Train model
        val classifier = new LinearSVC()
            .setMaxIter(20)
            .setRegParam(0.8)
            .setLabelCol("type")

        // Step - 4: Instantiate the One Vs Rest Classifier.
        val multiClassTrainer = new OneVsRest().setClassifier(classifier).setLabelCol("type")

        // Step - 5: Train the multiclass model.
        val model = multiClassTrainer.fit(output)


        // Step - 6: Print out all models
        model.models
            .map(e => e.asInstanceOf[LinearSVCModel])
            .foreach(
                mdl => println(s"Coefficients for specific model : ${mdl.coefficients} and intercept: ${mdl.intercept}"))


        val rawPredictions = model.transform(output)

        val predictions: DataFrame = Ex_2_Classified_with_Decision_Trees.enrichPredictions(spark, classNames, rawPredictions)

        predictions.show(100, true)

        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("type")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(rawPredictions)
        println("Test Error = " + (1.0 - accuracy))

    }
}
