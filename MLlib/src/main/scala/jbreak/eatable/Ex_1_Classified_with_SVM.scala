package jbreak.eatable

import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * I added one new column "eatable" and marked all observations in the dataset cyr_binarized_animals
  *
  * You can see that SVM doesn't work well.
  * The possible explanation: both classes are not linear separable.
  */
object Ex_1_Classified_with_SVM {
  def main(args: Array[String]): Unit = {

    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark_SQL")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val animals = spark.read
      .option("inferSchema", "true")
      .option("charset", "windows-1251")
      .option("header", "true")
      .csv("/home/zaleslaw/data/cyr_binarized_animals.csv")

    animals.show()

    val assembler = new VectorAssembler()
      .setInputCols(Array("legs", "tail"))
      .setOutputCol("features")

    /*   val assembler = new VectorAssembler()
           .setInputCols(Array("hair","feathers","eggs","milk","airborne","aquatic","predator","toothed","backbone","breathes","venomous","fins","legs","tail","domestic", "catsize"))
           .setOutputCol("features")
   */

    // Step - 2: Transform dataframe to vectorized dataframe
    val output = assembler.transform(animals).select("features", "eatable", "cyr_name")

    // Step - 3: Train model
    val trainer = new LinearSVC()
      .setMaxIter(20)
      .setRegParam(0.8)
      .setLabelCol("eatable")

    val model = trainer.fit(output)

    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    val rawPredictions = model.transform(output.sample(false, 0.2))

    val predictions: DataFrame = enrichPredictions(spark, rawPredictions)

    predictions.show(100, false)


    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("eatable")
      .setRawPredictionCol("prediction")
    val accuracy = evaluator.evaluate(predictions)
    println("Test set accuracy = " + accuracy)

  }

  def enrichPredictions(spark: SparkSession,
                        rawPredictions: DataFrame) = {
    import spark.implicits._

    val lambdaCheckClasses = (Type: Double, Prediction: Double) => {
      if (Type.equals(Prediction)) ""
      else "ERROR"
    }

    val checkClasses = spark.sqlContext.udf.register("isWorldWarTwoYear", lambdaCheckClasses)

    val predictions = rawPredictions.select(
      $"cyr_name".as("Name"),
      $"eatable",
      $"prediction")
      .withColumn("Error", checkClasses($"eatable", $"prediction"))
      .orderBy($"Error".desc)
    predictions
  }
}
