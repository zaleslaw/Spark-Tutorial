package RW.ToIgnite.pipeline

import jpoint.titanic.TitanicUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

/**
  * Predict surviving based on integer data
  * <p>
  * The main problem are nulls in data. Values to assemble (by VectorAssembler) cannot be null.
  */
object Ex_2_Supported_DT {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = TitanicUtils.getSparkSession

        val passengers = TitanicUtils.readPassengersWithCasting(spark)

        val imputer = new Imputer()
            .setInputCols(Array("pclass", "sibsp", "parch", "age", "fare"))
            .setOutputCols(Array("pclass", "sibsp", "parch", "age", "fare").map(c => s"${c}_imputed"))
            .setStrategy("mean")

        // Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler
        val assembler = new VectorAssembler()
            .setInputCols(Array("pclass_imputed", "sibsp_imputed", "parch_imputed", "age_imputed", "fare_imputed"))
            .setOutputCol("unscaled_features")

        val scaler = new MinMaxScaler() // new MaxAbsScaler()
            .setInputCol("unscaled_features")
            .setOutputCol("unnorm_features")

        val normalizer = new Normalizer()
            .setInputCol("unnorm_features")
            .setOutputCol("features")
            .setP(1.0)

        // Step - 3: Set up the Decision Tree Classifier
        val trainer = new DecisionTreeClassifier()
            .setLabelCol("survived")
            .setFeaturesCol("features")

        val pipeline:Pipeline = new Pipeline()
            .setStages(Array(imputer, assembler, scaler, normalizer, trainer))

        val model = pipeline.fit(passengers)


        model.write.overwrite().save("/home/zaleslaw/models/titanic/pipeline_supported_dt")

        // Step - 5: Predict with the model
        val rawPredictions = model.transform(passengers)

        // Step - 6: Evaluate prediction
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("survived")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")

        // Step - 7: Calculate accuracy
        val accuracy = evaluator.evaluate(rawPredictions)
        println("Test Error = " + (1.0 - accuracy))

    }
}
