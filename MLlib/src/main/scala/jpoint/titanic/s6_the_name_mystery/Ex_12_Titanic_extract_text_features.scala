package jpoint.titanic.s6_the_name_mystery

import jpoint.titanic.TitanicUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

/**
  * Let's extract text features from "name" column. Divide each string on separate "names" and build hashingTF model
  * <p>
  * Remove stop words with StopWordsRemover
  * <p>
  * Select features with PCA. Accuracy = 0.1428 and reduced with increasing of PCA from 50 to 1000
  * <p>
  * Increasing amount of features with decreasing of accuracy is an example of overfit.
  */
object Ex_12_Titanic_extract_text_features {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = TitanicUtils.getSparkSession

        val passengers = TitanicUtils.readPassengersWithCasting(spark)
            .select("survived", "pclass", "sibsp", "parch", "sex", "embarked", "age", "fare", "name")

        val regexTokenizer = new RegexTokenizer()
            .setInputCol("name")
            .setOutputCol("name_parts")
            .setPattern("\\w+").setGaps(false)

        val remover = new StopWordsRemover()
            .setStopWords(Array("mr", "mrs", "miss", "master", "jr", "j", "c", "d"))
            .setInputCol("name_parts")
            .setOutputCol("filtered_name_parts")

        val hashingTF = new HashingTF()
            .setInputCol("filtered_name_parts")
            .setOutputCol("text_features")
            .setNumFeatures(1000)

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
            .setOutputCol("features")

        val polyExpansion = new PolynomialExpansion()
            .setInputCol("features")
            .setOutputCol("polyFeatures")
            .setDegree(2)

        // We should join together text features and number features into one vector
        val assembler2 = new VectorAssembler()
            .setInputCols(Array("polyFeatures", "text_features"))
            .setOutputCol("joinedFeatures")

        val pca = new PCA()
            .setInputCol("joinedFeatures")
            .setK(100)
            .setOutputCol("pcaFeatures")

        val trainer = new DecisionTreeClassifier()
            .setLabelCol("survived")
            .setFeaturesCol("pcaFeatures")

        val pipeline:Pipeline = new Pipeline()
            .setStages(Array(regexTokenizer, remover, hashingTF, new TitanicUtils.Printer, sexIndexer, embarkedIndexer,
                imputer, assembler, polyExpansion, assembler2, pca, new TitanicUtils.Printer, trainer))

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
