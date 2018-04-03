package jpoint.titanic.s1_missed_values

import jpoint.titanic.TitanicUtils
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Fill the data with the default values presented in Map object
  */
object Ex_3_Titanic_filled_nulls_with_default_values {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = TitanicUtils.getSparkSession

        val passengers = TitanicUtils.readPassengers(spark)

        // Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler
        val assembler = new VectorAssembler()
            .setInputCols(Array("pclass", "sibsp", "parch"))
            .setOutputCol("features")

        // Step - 2: Define default values for missing data
        val replacements: Map[String,Any] = Map("pclass" -> 1, "sibsp" -> 0, "parch" -> 0)

        // Step - 3: Fill the data with the default values
        val passengersWithFilledEmptyValues = passengers.na.fill(replacements)

        passengersWithFilledEmptyValues.show() // <= check first row

        // Step - 4: Transform dataframe to vectorized dataframe
        val output = assembler.transform(
            passengersWithFilledEmptyValues
        ).select("features", "survived")

        // Step - 5: Set up the Decision Tree Classifier
        val trainer = new DecisionTreeClassifier()
            .setLabelCol("survived")
            .setFeaturesCol("features")

        // Step - 6: Train the model
        val model = trainer.fit(output)

        // Step - 7: Predict with the model
        val rawPredictions = model.transform(output)

        // Step - 8: Evaluate prediction
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("survived")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")

        // Step - 9: Calculate accuracy
        val accuracy = evaluator.evaluate(rawPredictions)
        println("Test Error = " + (1.0 - accuracy))

        // Step - 10: Print out the model
        val treeModel = model.asInstanceOf[DecisionTreeClassificationModel]
        println("Learned classification tree model:\n" + treeModel.toDebugString)
    }
}
