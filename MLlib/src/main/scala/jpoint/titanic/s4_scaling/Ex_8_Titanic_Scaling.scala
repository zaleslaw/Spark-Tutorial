package jpoint.titanic.s4_scaling

import jpoint.titanic.TitanicUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

/**
  * Add Scaler and Normalizer power. Accuracy = 0.1734
  * <p>
  * Q: How to print out intermediate results?
  * <p>
  * A: Add yet one custom transformer Printer
  */
object Ex_8_Titanic_Scaling {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = TitanicUtils.getSparkSession

        val passengers = TitanicUtils.readPassengersWithCasting(spark)
            .select("survived", "pclass", "sibsp", "parch", "sex", "embarked", "age", "fare")

        val sexIndexer = new StringIndexer()
            .setInputCol("sex")
            .setOutputCol("sexIndexed")
            .setHandleInvalid("keep")

        val embarkedIndexer = new StringIndexer()
            .setInputCol("embarked")
            .setOutputCol("embarkedIndexed")
            .setHandleInvalid("keep")

        val imputer = new Imputer()
            .setInputCols(Array("pclass", "sibsp", "parch", "age", "fare", "sexIndexed", "embarkedIndexed"))
            .setOutputCols(Array("pclass", "sibsp", "parch", "age", "fare", "sexIndexed", "embarkedIndexed").map(c => s"${c}_imputed"))
            .setStrategy("mean")

        val assembler = new VectorAssembler()
            .setInputCols(Array("pclass_imputed", "sibsp_imputed", "parch_imputed", "age_imputed", "fare_imputed", "sexIndexed_imputed", "embarkedIndexed_imputed"))
            .setOutputCol("unscaled_features")

        val scaler = new MinMaxScaler() // new MaxAbsScaler()
            .setInputCol("unscaled_features")
            .setOutputCol("unnorm_features")

        val normalizer = new Normalizer()
            .setInputCol("unnorm_features")
            .setOutputCol("features")
            .setP(1.0)

        val trainer = new DecisionTreeClassifier()
            .setLabelCol("survived")
            .setFeaturesCol("features")

        val pipeline:Pipeline = new Pipeline()
            .setStages(Array(sexIndexer, embarkedIndexer, imputer, assembler, scaler, normalizer, new TitanicUtils.Printer, trainer))

        val model = pipeline.fit(passengers)

        val rawPredictions = model.transform(passengers)

        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("survived")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")

        val accuracy = evaluator.evaluate(rawPredictions)
        println("Test Error = " + (1.0 - accuracy))
    }
}
