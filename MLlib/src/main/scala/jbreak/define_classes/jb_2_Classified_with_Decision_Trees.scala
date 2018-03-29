package jbreak.define_classes

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * This example lights usage of Decision Trees algorithm for Multi-class classification task.
  */
object jb_2_Classified_with_Decision_Trees {
  def main(args: Array[String]): Unit = {

    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark_SQL")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val (classNames, animalsWithClassTypeNames) = readAnimalsAndClassNames(spark)

    // Step - 1: Make Vectors from dataframe's columns using special VectorAssembler object
 /*       val assembler = new VectorAssembler()
         .setInputCols(Array("milk", "hair", "legs"))
         .setOutputCol("features")*/
    // Q: Why do we have so many birds?

    val assembler = new VectorAssembler()
      .setInputCols(Array("hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "toothed", "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"))
      .setOutputCol("features")

    // Step - 2: Transform dataframe to vectorized dataframe
    val output = assembler.transform(animalsWithClassTypeNames).select("features", "name", "type", "cyr_name", "Cyr_Class_Type")

    val trainer = new DecisionTreeClassifier()
      .setLabelCol("type")
      .setFeaturesCol("features")

    val model = trainer.fit(output)

    val rawPredictions = model.transform(output)

    val predictions: DataFrame = enrichPredictions(spark, classNames, rawPredictions)

    predictions.show(100)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("type")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(rawPredictions)
    println("Test Error = " + (1.0 - accuracy))

    val treeModel = model.asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)
  }

  /**
    * Read and parse two csv files to the tuple of dataframes.
    *
    * @param spark
    * @return Tuple of dataframes [classNames, animals]
    */
  def readAnimalsAndClassNames(spark: SparkSession): (DataFrame, DataFrame) = {
    val animals = spark.read
      .option("inferSchema", "true")
      .option("charset", "windows-1251")
      .option("header", "true")
      .csv("/home/zaleslaw/data/cyr_animals.csv")

    val classNames = spark.read
      .option("inferSchema", "true")
      .option("charset", "windows-1251")
      .option("header", "true")
      .csv("/home/zaleslaw/data/cyr_class.csv")

    val animalsWithClassTypeNames = animals.join(classNames, animals.col("type").equalTo(classNames.col("Class_Number")))

    (classNames, animalsWithClassTypeNames)
  }

  /**
    * Adds cyrillic class names to the raw prediction dataset, also it renames a few of columns and sort it.
    *
    * @param spark
    * @param classNames
    * @param rawPredictions
    * @return enriched predictions with class names
    */
  def enrichPredictions(spark: SparkSession,
                        classNames: DataFrame, rawPredictions: DataFrame) = {
    import spark.implicits._

    val prClassNames = classNames.select($"Class_Number".as("pr_class_id"), $"Cyr_Class_Type".as("pr_class_type"))
    val enrichedPredictions = rawPredictions.join(prClassNames, rawPredictions.col("prediction").equalTo(prClassNames.col("pr_class_id")))

    val lambdaCheckClasses = (Type: String, Prediction: String) => {
      if (Type.equals(Prediction)) ""
      else "ERROR"
    }

    val checkClasses = spark.sqlContext.udf.register("checkClasses", lambdaCheckClasses)

    val predictions = enrichedPredictions.select(
      $"name",
      $"cyr_name".as("Name"),
      $"Cyr_Class_Type".as("Real_class_type"),
      $"pr_class_type".as("Predicted_class_type"))
      .withColumn("Error", checkClasses($"Real_class_type", $"Predicted_class_type"))
      .orderBy($"Error".desc)

    predictions
  }
}
