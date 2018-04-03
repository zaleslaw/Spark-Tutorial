package jpoint.titanic.s5_feature_magic

import jpoint.titanic.TitanicUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.KNNClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

/**
  * Select features with PCA. Accuracy = 0.107 !!!! Let's use kNN
  */
object Ex_11_Titanic_knn_magic {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = TitanicUtils.getSparkSession

        val passengers = TitanicUtils.readPassengersWithCasting(spark)
            .select("survived", "pclass", "sibsp", "parch", "sex", "embarked", "age", "fare")

        val sexIndexer = new StringIndexer()
            .setInputCol("sex")
            .setOutputCol("sexIndexed")
            .setHandleInvalid("keep") // special mode to create special double value for null values

        val embarkedIndexer = new StringIndexer()
            .setInputCol("embarked")
            .setOutputCol("embarkedIndexed")
            .setHandleInvalid("keep") // special mode to create special double value for null values

        // Step - 1: Define default values for missing data
        val imputer = new Imputer()
            .setInputCols(Array("pclass", "sibsp", "parch", "age", "fare", "sexIndexed", "embarkedIndexed"))
            .setOutputCols(Array("pclass", "sibsp", "parch", "age", "fare", "sexIndexed", "embarkedIndexed").map(c => s"${c}_imputed"))
            .setStrategy("mean")

        // Step - 2: Make Vectors from dataframe's columns using special Vector Assmebler
        val assembler = new VectorAssembler()
            .setInputCols(Array("pclass_imputed", "sibsp_imputed", "parch_imputed", "age_imputed", "fare_imputed", "sexIndexed_imputed", "embarkedIndexed_imputed"))
            .setOutputCol("features")

        val polyExpansion = new PolynomialExpansion()
            .setInputCol("features")
            .setOutputCol("polyFeatures")
            .setDegree(2)

        val pca = new PCA()
            .setInputCol("polyFeatures")
            .setK(15)
            .setOutputCol("pcaFeatures")

        val trainer = new KNNClassifier()
            .setTopTreeSize(3)
            .setK(1)
            .setLabelCol("survived")
            .setFeaturesCol("pcaFeatures")

        val pipeline:Pipeline = new Pipeline()
            .setStages(Array(sexIndexer, embarkedIndexer, imputer, assembler, polyExpansion, pca, new TitanicUtils.Printer, trainer))

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
