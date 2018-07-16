package bootcamp

import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel, linalg}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Step_6_Choose_best_model {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = getSession

    val preTrain: Dataset[Row] = makeFeatures(spark)

    val class_01 = preTrain.filter("label == 0")
    val class_0 = class_01.coalesce(1)
    class_0.cache()

    val class_11 = preTrain.filter("label == 1")
    val class_1 = class_11.coalesce(1)
    class_1.cache()


    val datasets = class_0
        .randomSplit(Array(0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05))
        //.randomSplit(Array(0.25, 0.25, 0.25, 0.25))

    var roc_auc_counter = 0.0
    var bestModel: PipelineModel = null

    for (dataset <- datasets) {

      val featuredData = dataset.union(class_1)

      featuredData.cache()

      val predictionSet: DataFrame = readPredictionSet(spark)

      val Array(train, test) = featuredData.randomSplit(Array(0.7, 0.3), seed = 12345)
      train.cache()
      test.cache()

      val assembler = new VectorAssembler()
          .setInputCols(Array(
            "max_type",
            "sum_diff",
            "max_diff",
            "min_diff",
            "avg_diff",
            "amount_of_records"
          ))
          .setOutputCol("unscaled_features")

      val scaler = new MinMaxScaler() // new MaxAbsScaler()
          .setInputCol("unscaled_features")
          .setOutputCol("features")
/*
      val trainer = new KNNClassifier()
          .setK(11)
          .setLabelCol("label")
          .setFeaturesCol("features")*/

      val trainer = new RandomForestClassifier()
          .setMaxDepth(6)
          .setFeatureSubsetStrategy("all")
          //.setSubsamplingRate(0.6)
          .setLabelCol("label")
          .setFeaturesCol("features")

      val pipeline:Pipeline = new Pipeline()
          .setStages(Array(assembler, scaler, trainer))

      val evaluator = new BinaryClassificationEvaluator()
          .setLabelCol("label")
          .setMetricName("areaUnderROC")

      val model = pipeline.fit(train)

      val rawPredictions = model.transform(test)

      bestModel = model
      val roc_auc = evaluator.evaluate(rawPredictions)
      println("ROC AUC = " + roc_auc)

      roc_auc_counter += roc_auc

      featuredData.unpersist()
    }

    println("Avg roc_auc = " + (roc_auc_counter/20))


    val predictionSet: DataFrame = readPredictionSet(spark)
    val prediction = bestModel.transform(predictionSet)
    prediction.cache()
    prediction.show(100, truncate = false)

    prediction.groupBy("prediction").count().show()

    writeResult(spark, prediction)
  }

  private def writeResult(spark: SparkSession, prediction: DataFrame) = {
    import org.apache.spark.sql.functions.udf
    import spark.implicits._

    val first = udf((v: linalg.Vector) => v.toArray(0))
    val second = udf((v: linalg.Vector) => v.toArray(1))
    prediction
        //.withColumn("prob1", first($"probability"))
        .withColumn("prob2", second($"probability"))
        .drop("probability")
        .select("id", "prob2")
        .withColumn("prob2", $"prob2".cast(sql.types.StringType))
        .coalesce(1)
        .write
        .mode("overwrite")
        .csv("C:\\home\\bootcamp\\result")
  }

  private def makeFeatures(spark: SparkSession) = {
    import spark.implicits._
    val trainRaw = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .parquet("C:\\home\\bootcamp\\train")
        .withColumn("type", $"type".cast(sql.types.IntegerType))
        .withColumn("diff", $"diff".cast(sql.types.IntegerType))
        .withColumn("target", $"target".cast(sql.types.DoubleType))

    import org.apache.spark.sql.functions._

    val preTrain = trainRaw
        .groupBy("id")
        .agg(max("type") as "max_type",
          max("target") as "label",
          max("diff") as "max_diff",
          min("diff") as "min_diff",
          avg("diff") as "avg_diff",
          sum("diff") as "sum_diff",
          count("id") as "amount_of_records")
    // add select
    preTrain
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
          sum("diff") as "sum_diff",
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
