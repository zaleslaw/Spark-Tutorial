package jpoint.titanic.s1_missed_values

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Choose strategy to work with null data. Accuracy =  0.288
  */
object Ex_2_Titanic_without_nulls {
    def main(args: Array[String]): Unit = {

        //For windows only: don't forget to put winutils.exe to c:/bin folder
        System.setProperty("hadoop.home.dir", "c:\\")

        val spark = SparkSession.builder
            .master("local")
            .appName("Spark_SQL")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val passengers = readPassengers(spark)

        // Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler
        val assembler = new VectorAssembler()
            .setInputCols(Array("pclass", "sibsp", "parch"))
            .setOutputCol("features")

        // Step - 2: Transform dataframe to vectorized dataframe with dropping rows
        val output = assembler.transform(
            passengers.na.drop(Array("pclass", "sibsp", "parch")) // <============== drop row if it has nulls/NaNs in the next list of columns
        ).select("features", "survived")

        passengers.na.drop()

        val trainer = new DecisionTreeClassifier()
            .setLabelCol("survived")
            .setFeaturesCol("features")

        val model = trainer.fit(output)

        val rawPredictions = model.transform(output)

        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("survived")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")

        val accuracy = evaluator.evaluate(rawPredictions)
        println("Test Error = " + (1.0 - accuracy))

        val treeModel = model.asInstanceOf[DecisionTreeClassificationModel]
        println("Learned classification tree model:\n" + treeModel.toDebugString)
    }

    def readPassengers(spark: SparkSession): DataFrame = {
        val passengers = spark.read
            .option("delimiter", ";")
            .option("inferSchema", "true")
            .option("header", "true")
            .csv("/home/zaleslaw/data/titanic.csv")

        passengers.printSchema()

        passengers.show()

        passengers
    }
}
