package jpoint.titanic.s2_categorial

import jpoint.titanic.TitanicUtils
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{Imputer, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Add two features: "sex" and "embarked". They are presented as sets of string. Accuracy = 0,194
  * <p>
  * Old columns should be dropped from the dataset to use imputer
  * <p>
  * The first row in imputed dataset is filled with the special values
  */
object Ex_5_Titanic_use_categorial_feature {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = TitanicUtils.getSparkSession

        val passengers = TitanicUtils.readPassengersWithCastingToDoubles(spark)
            .select("survived", "pclass", "sibsp", "parch", "sex", "embarked")

        // Step - 1: Define the Indexer for the column "sex"
        val sexIndexer = new StringIndexer()
            .setInputCol("sex")
            .setOutputCol("sexIndexed")
            .setHandleInvalid("keep") // special mode to create special double value for null values

        val passengersWithIndexedSex = sexIndexer.fit(passengers).transform(passengers)

        // Step - 2: Define the Indexer for the column "embarked"
        val embarkedIndexer = new StringIndexer()
            .setInputCol("embarked")
            .setOutputCol("embarkedIndexed")
            .setHandleInvalid("keep") // special mode to create special double value for null values

        val passengersWithIndexedCategorialFeatures = embarkedIndexer
            .fit(passengersWithIndexedSex)
            .transform(passengersWithIndexedSex)
           .drop("sex", "embarked") // <============== drop columns to use Imputer

        passengersWithIndexedCategorialFeatures.show()
        passengersWithIndexedCategorialFeatures.printSchema()

        // Step - 3: Define strategy and new column names for Imputer transformation
        val imputer = new Imputer()
            .setInputCols(passengersWithIndexedCategorialFeatures.columns)
            .setOutputCols(passengersWithIndexedCategorialFeatures.columns.map(c => s"${c}_imputed"))
            .setStrategy("mean")

        val passengersWithFilledEmptyValues = imputer.fit(passengersWithIndexedCategorialFeatures).transform(passengersWithIndexedCategorialFeatures)

        println("Special values created by imputer and encoder in the first row")
        passengersWithFilledEmptyValues.show(1)

        passengersWithFilledEmptyValues.show() // <= check first row

        // Step - 4: Make Vectors from dataframe's columns using special Vector Assmebler
        val assembler = new VectorAssembler()
            .setInputCols(Array("pclass_imputed", "sibsp_imputed", "parch_imputed", "sexIndexed_imputed", "embarkedIndexed_imputed"))
            .setOutputCol("features")

        // Step - 5: Transform dataframe to vectorized dataframe with dropping rows
        val output = assembler.transform(
            passengersWithFilledEmptyValues
        ).select("features", "survived")

        // Step - 6: Set up the Decision Tree Classifier
        val trainer = new DecisionTreeClassifier()
            .setLabelCol("survived")
            .setFeaturesCol("features")

        // Step - 7: Train the model
        val model = trainer.fit(output)

        // Step - 8: Predict with the model
        val rawPredictions = model.transform(output)

        // Step - 9: Evaluate prediction
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("survived")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")

        // Step - 10: Calculate accuracy
        val accuracy = evaluator.evaluate(rawPredictions)
        println("Test Error = " + (1.0 - accuracy))

        // Step - 11: Print out the model
        val treeModel = model.asInstanceOf[DecisionTreeClassificationModel]
        println("Learned classification tree model:\n" + treeModel.toDebugString)
    }
}
