package jbreak.classes

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
  * Try to find clusters in small dataset and compare it with real classes
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
            .option("header", "true")
            .csv("/home/zaleslaw/data/animals.csv")

        // from 2-dimension space to 16-dimension space improves the prediction
        val assembler = new VectorAssembler()
            .setInputCols(Array("hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "toothed", "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"))
            .setOutputCol("features")

        // Step - 2: Transform dataframe to vectorized dataframe
        val output = assembler.transform(animals).select("features", "name", "type")

        val trainer = new DecisionTreeClassifier()
            .setLabelCol("type")
            .setFeaturesCol("features")

        val model = trainer.fit(output)

        val predictions = model.transform(output)

        predictions.select("name", "prediction", "type", "features").show(100)

        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("type")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println("Test Error = " + (1.0 - accuracy))

        val treeModel = model.asInstanceOf[DecisionTreeClassificationModel]
        println("Learned classification tree model:\n" + treeModel.toDebugString)
    }
}
