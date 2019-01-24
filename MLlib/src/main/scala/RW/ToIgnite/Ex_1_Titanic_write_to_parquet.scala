package RW.ToIgnite

import jpoint.titanic.TitanicUtils
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
  * Predict surviving based on integer data
  * <p>
  * The main problem are nulls in data. Values to assemble (by VectorAssembler) cannot be null.
  */
object Ex_1_Titanic_write_to_parquet {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = TitanicUtils.getSparkSession

        val passengers = TitanicUtils.readPassengers(spark)

        // Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler
        val assembler = new VectorAssembler()
            .setInputCols(Array("pclass", "sibsp", "parch"))
            .setOutputCol("features")

        // Step - 2: Transform dataframe to vectorized dataframe with dropping rows
        val output = assembler.transform(
            passengers.na.drop(Array("pclass", "sibsp", "parch")) // <============== drop row if it has nulls/NaNs in the next list of columns
        ).select("features", "survived")

        val trainer = new LogisticRegression()
            .setMaxIter(100)
            .setRegParam(0.1)
            .setElasticNetParam(0.1)
            .setThreshold(0.55)
            .setLabelCol("survived")
            .setFeaturesCol("features")


        // Step - 4: Train the model
        val model = trainer.fit(output)
        model.write.overwrite().save("/home/zaleslaw/models/titanic")
        println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

        // Step - 5: Predict with the model
        val rawPredictions = model.transform(output)

        // Step - 6: Evaluate prediction
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("survived")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")

        // Step - 7: Calculate accuracy
        val accuracy = evaluator.evaluate(rawPredictions)
        println("Test Error = " + (1.0 - accuracy))


    }
}
