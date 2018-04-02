package jpoint.titanic.s8_cross_validation

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Select features with PCA. Accuracy > 0.2 and increasing with increasing of PCA from 100 to 1000
  */
object Ex_15_Titanic_cross_validation {
    def main(args: Array[String]): Unit = {

        //For windows only: don't forget to put winutils.exe to c:/bin folder
        System.setProperty("hadoop.home.dir", "c:\\")

        val spark = SparkSession.builder
            .master("local")
            .appName("Spark_SQL")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val passengers = readPassengers(spark)
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

        println("---------- The best model is ----------")
        println("Num of features " + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[HashingTF].getNumFeatures)
        println("Amount of components in PCA " + cvModel.bestModel.asInstanceOf[PipelineModel].stages(9).asInstanceOf[PCAModel].getK)

        val rawPredictions = cvModel.transform(test)

        val accuracy = evaluator.evaluate(rawPredictions)
        println("Test Error = " + (1.0 - accuracy))
    }

    def readPassengers(spark: SparkSession): DataFrame = {
        val passengers = spark.read
            .option("delimiter", ";")
            .option("inferSchema", "true")
            .option("header", "true")
            .csv("/home/zaleslaw/data/titanic.csv")

        import org.apache.spark.sql
        import spark.implicits._

        val castedPassengers = passengers
            .withColumn("survived", $"survived".cast(sql.types.DoubleType))
            .withColumn("pclass", $"pclass".cast(sql.types.DoubleType))
            .withColumn("sibsp", $"sibsp".cast(sql.types.DoubleType))
            .withColumn("parch", $"parch".cast(sql.types.DoubleType))
            .withColumn("age", $"age".cast(sql.types.DoubleType))
            .withColumn("fare", $"fare".cast(sql.types.DoubleType))

        castedPassengers.printSchema()

        castedPassengers.show(false)

        castedPassengers
    }

    class DropSex extends Transformer {
        private val serialVersionUID = 5545470640951989469L

        override def transform(
            dataset: Dataset[_]): DataFrame = {
            val result = dataset.drop("sex", "embarked") // <============== drop columns to use Imputer
            result.show()
            result.printSchema()
            result
        }

        override def copy(
            extra: ParamMap): Transformer = null

        override def transformSchema(
            schema: StructType): StructType = schema

        override val uid: String = "CustomTransformer" + serialVersionUID
    }
}
