package bootcamp

import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, linalg}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Step_4_Choose_best_model_2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = getSession

    val featuredData: Dataset[Row] = makeFeatures(spark)
    val predictionSet: DataFrame = readPredictionSet(spark)

    val Array(train, test) = featuredData.randomSplit(Array(0.7, 0.3), seed = 12345)

    train.cache()
    test.cache()

    val assembler = new VectorAssembler()
      .setInputCols(Array("max_type", "max_diff", "min_diff",  "avg_diff", "amount_of_records"))
      .setOutputCol("unscaled_features")

    val scaler = new MinMaxScaler() // new MaxAbsScaler()
        .setInputCol("unscaled_features")
        .setOutputCol("unnorm_features")

/*    val normalizer = new Normalizer()
        .setInputCol("unnorm_features")
        .setOutputCol("features")
        .setP(2.0)*/

    val layers = Array[Int](5, 4, 3, 2)

    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
        .setLayers(layers)
        .setSeed(1234L)
        .setMaxIter(100)
        .setLabelCol("label")
        .setFeaturesCol("unnorm_features")

    val pipeline:Pipeline = new Pipeline()
      .setStages(Array(assembler, scaler, trainer))

    val evaluator = new BinaryClassificationEvaluator()
        .setRawPredictionCol("prediction")
        .setLabelCol("label")
        .setMetricName("areaUnderROC")

    val model = pipeline.fit(train)

    val rawPredictions = model.transform(test)
    rawPredictions.show(100, truncate = false)

    val roc_auc = evaluator.evaluate(rawPredictions)
    println("ROC AUC = " + roc_auc)

    val prediction = model.transform(predictionSet)
    prediction.cache()
    prediction.show(100, truncate = false)

    writeResult(spark, prediction)
  }

  private def writeResult(spark: SparkSession, prediction: DataFrame) = {
    import org.apache.spark.sql.functions.udf
    import spark.implicits._

    val first = udf((v: linalg.Vector) => v.toArray(0))
    val second = udf((v: linalg.Vector) => v.toArray(1))
    prediction

       // .withColumn("prob2", second($"rawPrediction"))
       // .drop("probability")
        .select("id", "prediction")
        .withColumn("prediction", $"prediction".cast(sql.types.StringType))
        .coalesce(1)
        .write
        .mode("overwrite")
        .csv("C:\\home\\bootcamp\\result2")
  }

  private def makeFeatures(spark: SparkSession) = {
    import spark.implicits._
    val trainRaw = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .parquet("C:\\home\\bootcamp\\train")
        .withColumn("type", $"type".cast(sql.types.IntegerType))
        .withColumn("diff", $"diff".cast(sql.types.IntegerType))
        .withColumn("target", $"target".cast(sql.types.IntegerType))

    import org.apache.spark.sql.functions._

    val preTrain = trainRaw
        .groupBy("id")
        .agg(max("type") as "max_type",
          max("target") as "label",
          max("diff") as "max_diff",
          min("diff") as "min_diff",
          avg("diff") as "avg_diff",
          count("id") as "amount_of_records")
    // add select

    val train = preTrain.filter("label == 0").sample(false, 0.05, 1234L).union(preTrain.filter("label == 1"))
    train.cache()
    train.groupBy("label").count().show()
    train
  }

  private def readPredictionSet(spark: SparkSession): DataFrame = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val testRaw = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .parquet("C:\\home\\bootcamp\\test")
        .withColumn("type", $"type".cast(sql.types.IntegerType))
        .withColumn("diff", $"diff".cast(sql.types.IntegerType))

    val test = testRaw
        .groupBy("id")
        .agg(max("type") as "max_type",
          max("diff") as "max_diff",
          min("diff") as "min_diff",
          avg("diff") as "avg_diff",
          count("id") as "amount_of_records")
    // add select
    test.cache()
    test
  }
  private def getSession = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark_SQL")
      .config("spark.executor.memory", "4g")
        .config("spark.executor.instances", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }
}
