package jpoint.titanic.s1_missed_values

import jpoint.titanic.TitanicUtils
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{Imputer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Fill missed values with average values
  *
  * But imputer needs in Double values in the whole dataset. Accuracy =  0.288
  */
object Ex_4_Titanic_filled_nulls_with_average {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = TitanicUtils.getSparkSession

        val passengers = TitanicUtils.readPassengersWithCastingToDoubles(spark)
            .select("survived", "pclass", "sibsp", "parch")

        // Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler
        val assembler = new VectorAssembler()
            .setInputCols(Array("pclass_imputed", "sibsp_imputed", "parch_imputed"))
            .setOutputCol("features")

        // Step - 2: Define strategy and new column names for Imputer transformation
        val imputer = new Imputer()
            .setInputCols(passengers.columns)
            .setOutputCols(passengers.columns.map(c => s"${c}_imputed"))
            .setStrategy("mean")

        // Step - 3: Transform the dataset with the Imputer
        val passengersWithFilledEmptyValues = imputer.fit(passengers).transform(passengers)

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
