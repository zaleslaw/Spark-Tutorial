package jpoint.titanic.s8_cross_validation

import jpoint.titanic.TitanicUtils
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession


/**
  * First of all, this example use different set of parameters to find the best model (pipeline).
  * In the second, it includes K-fold cross-validation on train dataset to get
  */
object Ex_15_Titanic_cross_validation {
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

        val trainer = new RandomForestClassifier()
            .setLabelCol("survived")
            .setFeaturesCol("pcaFeatures")
            .setMaxDepth(20)
            .setNumTrees(200)

        val pipeline:Pipeline = new Pipeline()
            .setStages(Array(regexTokenizer, remover, hashingTF, sexIndexer, embarkedIndexer, imputer, assembler, polyExpansion, assembler2, pca, trainer))

        val paramGrid = new ParamGridBuilder()
            .addGrid(hashingTF.numFeatures, Array(100, 1000))
            //.addGrid(imputer.strategy, Array("mean", "median"))
            //.addGrid(polyExpansion.degree, Array(2, 3))
            .addGrid(pca.k, Array(10, 100))
            .build()

        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("survived")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")

        val cv = new CrossValidator()
            .setEstimator(pipeline)
            .setEvaluator(evaluator)
            .setEstimatorParamMaps(paramGrid)
            .setNumFolds(3)

        // Run cross-validation, and choose the best set of parameters.
        val cvModel = cv.fit(training)

        println("---------- The best model's parameters are ----------")
        println("Num of features " + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[HashingTF].getNumFeatures)
        println("Amount of components in PCA " + cvModel.bestModel.asInstanceOf[PipelineModel].stages(9).asInstanceOf[PCAModel].getK)

        val rawPredictions = cvModel.transform(test)

        val accuracy = evaluator.evaluate(rawPredictions)
        println("Test Error = " + (1.0 - accuracy))
    }
}
