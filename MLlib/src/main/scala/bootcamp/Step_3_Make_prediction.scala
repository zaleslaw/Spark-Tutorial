package bootcamp

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object Step_3_Make_prediction {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = getSession

    import spark.implicits._

    val trainRaw = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .parquet("C:\\Users\\alexey_zinovyev\\Downloads\\mlboot_dataset\\train")
      .withColumn("type", $"type".cast(sql.types.IntegerType))
      .withColumn("diff", $"diff".cast(sql.types.IntegerType))
      .withColumn("target", $"target".cast(sql.types.IntegerType))

    trainRaw.cache()
    trainRaw.show(false)

    val testRaw = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .parquet("C:\\Users\\alexey_zinovyev\\Downloads\\mlboot_dataset\\test")
      .withColumn("type", $"type".cast(sql.types.IntegerType))
      .withColumn("diff", $"diff".cast(sql.types.IntegerType))

    testRaw.cache()
    testRaw.show(false)

    import org.apache.spark.sql.functions._

    val preTrain = trainRaw
      .groupBy("id")
      .agg(max("type") as "max_type", max("target") as "label", max("diff") as "max_diff", avg("diff") as "avg_diff", count("id") as "amount_of_records")
// add select

    val train = preTrain.filter("label == 0").sample(false, 0.05, 1234L).union(preTrain.filter("label == 1"))
    train.cache()
    train.groupBy("label").count().show()
    //move before previous step

    val test = testRaw
      .groupBy("id")
      .agg(max("type") as "max_type", max("diff") as "max_diff", avg("diff") as "avg_diff", count("id") as "amount_of_records")
// add select

    val assembler = new VectorAssembler()
      .setInputCols(Array("max_type", "max_diff", "avg_diff", "amount_of_records"))
      .setOutputCol("features")

    val trainer = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")


    val pipeline:Pipeline = new Pipeline()
      .setStages(Array(assembler, trainer))

    val model = pipeline.fit(train)

    val rawPredictions = model.transform(train)
    rawPredictions.show(100, truncate = false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(rawPredictions)
    println("Test Error = " + (1.0 - accuracy))


    val testPrediction = model.transform(test)
    testPrediction.cache()
    testPrediction.show(100, truncate = false)




    import org.apache.spark.mllib.linalg.Vector
    import org.apache.spark.sql.functions.udf


    val first = udf((v: org.apache.spark.ml.linalg.Vector) => v.toArray(0))
    val second = udf((v: org.apache.spark.ml.linalg.Vector) => v.toArray(1))
    testPrediction
      //.withColumn("prob1", first($"probability"))
      .withColumn("prob2", second($"probability"))
      .drop("probability")
      .select("id", "prob2")
      .withColumn("prob2", $"prob2".cast(sql.types.StringType))
      .coalesce(1)
      .write
      .csv("C:\\Users\\alexey_zinovyev\\Downloads\\mlboot_dataset\\result")


  }

  private def getSession = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark_SQL")
      .config("spark.executor.memory", "2g")
      .config("spark.cores.max", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }
}
