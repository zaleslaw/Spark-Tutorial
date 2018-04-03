package jpoint.titanic.s6_the_name_mystery

import jpoint.titanic.TitanicUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

/**
  * Wow, but what's the main trouble with our accuracy metric? Our prediction is better and better, but it can be sort of overfit.
  * <p>
  * Split data and test and train datasets and re-run our algorithms (of course it has kind of randomization)
  * <p>
  * Select features with PCA. Accuracy > 0.23 and increasing with increasing of PCA from 100 to 1000 (to 0,3)
  */
object Ex_13_Titanic_split_test_train_data {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = TitanicUtils.getSparkSession

        val passengers = TitanicUtils.readPassengersWithCasting(spark)
            .select("survived", "pclass", "sibsp", "parch", "sex", "embarked", "age", "fare", "name")

        val Array(training, test) = passengers.randomSplit(Array(0.7, 0.3), seed = 12345)

        training.cache()
        test.cache()

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

        val assembler2 = new VectorAssembler()
            .setInputCols(Array("polyFeatures", "text_features"))
            .setOutputCol("joinedFeatures")

        val pca = new PCA()
            .setInputCol("joinedFeatures")
            .setK(100) // change on 1000
            .setOutputCol("pcaFeatures")

        val trainer = new DecisionTreeClassifier()
            .setLabelCol("survived")
            .setFeaturesCol("pcaFeatures")

        val pipeline:Pipeline = new Pipeline()
            .setStages(Array(regexTokenizer, remover, hashingTF, sexIndexer, embarkedIndexer,
                imputer, assembler, polyExpansion, assembler2, pca, trainer))

        val model = pipeline.fit(training)

        val rawPredictions = model.transform(test)

        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("survived")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")

        val accuracy = evaluator.evaluate(rawPredictions)
        println("Test Error = " + (1.0 - accuracy))
    }
}
