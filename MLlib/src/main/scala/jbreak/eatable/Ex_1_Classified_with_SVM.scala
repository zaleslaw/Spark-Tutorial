package jbreak.eatable

import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
  * Try to find clusters in small dataset and compare it with real classes
  */
object Ex_1_Classified_with_SVM {
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

        animals.show()

        val assembler = new VectorAssembler()
            .setInputCols(Array("legs", "tail"))
            .setOutputCol("features")

        /*   val assembler = new VectorAssembler()
               .setInputCols(Array("hair","feathers","eggs","milk","airborne","aquatic","predator","toothed","backbone","breathes","venomous","fins","legs","tail","domestic", "catsize"))
               .setOutputCol("features")
       */

        // Step - 2: Transform dataframe to vectorized dataframe
        val output = assembler.transform(animals).select("features", "name", "type", "eatable")

        // Step - 3: Train model
        val trainer = new LinearSVC()
            .setMaxIter(20)
            .setRegParam(0.8)
            .setLabelCol("eatable")

        val model = trainer.fit(output)

        println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

        val predictions = model.transform(output.sample(false, 0.2))
        predictions.show(100, true)

        val evaluator = new BinaryClassificationEvaluator()
            .setLabelCol("eatable")
            .setRawPredictionCol("prediction")
        val accuracy = evaluator.evaluate(predictions)
        println("Test set accuracy = " + accuracy)

    }
}
