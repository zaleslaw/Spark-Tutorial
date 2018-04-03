package jpoint.titanic.s1_missed_values

import jpoint.titanic.TitanicUtils
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Predict surviving based on integer data
  * <p>
  * The main problem are nulls in data. Values to assemble (by VectorAssembler) cannot be null.
  */
object Ex_1_Titanic_with_nulls {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = TitanicUtils.getSparkSession

        val passengers = TitanicUtils.readPassengers(spark)

        // Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler
        val assembler = new VectorAssembler()
            .setInputCols(Array("pclass", "sibsp", "parch"))
            .setOutputCol("features")

        // Step - 2: Transform dataframe to vectorized dataframe
        val output = assembler
            .transform(passengers)
            .select("features", "survived")

        // Step - 3: Set up the Decision Tree Classifier
        val trainer = new DecisionTreeClassifier()
            .setLabelCol("survived")
            .setFeaturesCol("features")

        // Step - 4: Train the model
        val model = trainer.fit(output)

        // ERROR: we will take an org.apache.spark.SparkException: Failed to execute user defined function($anonfun$3: (struct<pclass_double_vecAssembler_ec192f19574f:double
        // Caused by: org.apache.spark.SparkException: Values to assemble cannot be null.
        val rawPredictions = model.transform(output)

        // Step - 5: Evaluate prediction
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("survived")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")

        // Step - 6: Calculate accuracy
        val accuracy = evaluator.evaluate(rawPredictions)
        println("Test Error = " + (1.0 - accuracy))

        // Step - 7: Print out the model
        val treeModel = model.asInstanceOf[DecisionTreeClassificationModel]
        println("Learned classification tree model:\n" + treeModel.toDebugString)
    }
}
