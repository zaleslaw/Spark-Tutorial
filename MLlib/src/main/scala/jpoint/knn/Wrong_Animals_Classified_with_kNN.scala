package jpoint.knn

import org.apache.spark.ml.classification.KNNClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * This kNN algorithm has the best prediction. On the same dataset, of course
  */
object Wrong_Animals_Classified_with_kNN {
    def main(args: Array[String]): Unit = {

        //For windows only: don't forget to put winutils.exe to c:/bin folder
        System.setProperty("hadoop.home.dir", "c:\\")

        val spark = SparkSession.builder
            .master("local")
            .appName("Spark_SQL")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val (classNames, animalsWithClassTypeNames) = readAnimalsAndClassNames(spark)

        // Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler
        val assembler = new VectorAssembler()
            .setInputCols(Array("hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "toothed", "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"))
            .setOutputCol("features")

        // Step - 2: Transform dataframe to vectorized dataframe
        val output = assembler.transform(animalsWithClassTypeNames).select("features", "name", "type", "cyr_name", "Cyr_Class_Type")

        val knn = new KNNClassifier()
            .setTopTreeSize(3)
            .setK(1)
            .setLabelCol("type")
            .setFeaturesCol("features")

        output.printSchema()
        val model = knn.fit(output)

        val rawPredictions = model.transform(output)
        rawPredictions.show()

        val predictions: DataFrame = enrichPredictions(spark, classNames, rawPredictions)

        predictions.show(100)

        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("type")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")

        val accuracy = evaluator.evaluate(rawPredictions)
        println("Test Error = " + (1.0 - accuracy))
    }

    /**
      * Read and parse two csv files to the tuple of dataframes.
      * @param spark
      * @return Tuple of dataframes [classNames, animals]
      */
    def readAnimalsAndClassNames(spark: SparkSession): (DataFrame, DataFrame) = {
/*
        val dataSchema = new StructType()
            .add("name","string")
            .add("cyr_name","string")
            .add("hair","int")
            .add("feathers","int")
            .add("eggs", "int")
            .add("milk", "int")
            .add("airborne", "int")
            .add("aquatic", "int")
            .add("predator", "int")
            .add("toothed", "int")
            .add("backbone", "int")
            .add("breathes", "int")
            .add("venomous", "int")
            .add("fins", "int")
            .add("legs", "int")
            .add("tail", "int")
            .add("domestic", "int")
            .add("catsize", "int")
            .add("type", "double")
*/

        val animals = spark.read
            .option("inferSchema", "true") // change to false
            //.schema(dataSchema)
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
      * @param spark
      * @param classNames
      * @param rawPredictions
      * @return enriched predictions with class names
      */
    def enrichPredictions(spark: SparkSession,
        classNames: DataFrame, rawPredictions: DataFrame): Dataset[Row] = {
        import spark.implicits._

        val prClassNames = classNames.select($"Class_Number".as("pr_class_id"), $"Cyr_Class_Type".as("pr_class_type"))
        val enrichedPredictions = rawPredictions.join(prClassNames, rawPredictions.col("prediction").equalTo(prClassNames.col("pr_class_id")))

        val lambdaCheckClasses = (Type: String, Prediction: String) => {
            if (Type.equals(Prediction)) ""
            else "ERROR"
        }

        val checkClasses = spark.sqlContext.udf.register("isWorldWarTwoYear", lambdaCheckClasses)

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
