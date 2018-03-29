package jpoint.titanic.s1_missed_values

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{Imputer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Fill missed values with average values
  *
  * But imputer needs in Double values in the whole dataset. Accuracy =  0.288
  */
object Ex_4_Titanic_filled_nulls_with_average {
    def main(args: Array[String]): Unit = {

        //For windows only: don't forget to put winutils.exe to c:/bin folder
        System.setProperty("hadoop.home.dir", "c:\\")

        val spark = SparkSession.builder
            .master("local")
            .appName("Spark_SQL")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val passengers = readPassengers(spark)
            .select("survived", "pclass", "sibsp", "parch")


        // Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler
        val assembler = new VectorAssembler()
            .setInputCols(Array("pclass_imputed", "sibsp_imputed", "parch_imputed"))
            .setOutputCol("features")

        // Step - 2: Define default values for missing data
        val imputer = new Imputer()
            .setInputCols(passengers.columns)
            .setOutputCols(passengers.columns.map(c => s"${c}_imputed"))
            .setStrategy("mean")

        val passengersWithFilledEmptyValues = imputer.fit(passengers).transform(passengers)

        passengersWithFilledEmptyValues.show() // <= check first row

        // Step - 3: Transform dataframe to vectorized dataframe with dropping rows
        val output = assembler.transform(
            passengersWithFilledEmptyValues // <============== drop row if it has nulls/NaNs in the next list of columns
        ).select("features", "survived")

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

        import org.apache.spark.sql
        import spark.implicits._

        val castedPassengers = passengers
            .withColumn("survived", $"survived".cast(sql.types.DoubleType))
            .withColumn("pclass", $"pclass".cast(sql.types.DoubleType))
            .withColumn("sibsp", $"sibsp".cast(sql.types.DoubleType))
            .withColumn("parch", $"parch".cast(sql.types.DoubleType))

        castedPassengers.printSchema()

        castedPassengers.show()

        castedPassengers
    }
}
