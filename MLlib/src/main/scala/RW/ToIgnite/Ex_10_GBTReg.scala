package RW.ToIgnite

import jpoint.titanic.TitanicUtils
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression._
import org.apache.spark.sql.SparkSession

/**
  * Predict surviving based on integer data
  * <p>
  * The main problem are nulls in data. Values to assemble (by VectorAssembler) cannot be null.
  */
object Ex_9_RFReg {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = TitanicUtils.getSparkSession

        val passengers = TitanicUtils.readPassengersWithCasting(spark)
            .select("survived", "pclass", "sibsp", "parch", "sex", "embarked", "age")

        // Step - 1: Make Vectors from dataframe's columns using special Vector Assmebler
        val assembler = new VectorAssembler()
            .setInputCols(Array("pclass", "sibsp", "parch", "survived"))
            .setOutputCol("features")

        // Step - 2: Transform dataframe to vectorized dataframe with dropping rows
        val output = assembler.transform(
            passengers.na.drop(Array("pclass", "sibsp", "parch", "survived", "age")) // <============== drop row if it has nulls/NaNs in the next list of columns
        ).select("features", "age")

        val trainer = new GBTRegressor()
            .setMaxIter(100)
            .setMaxDepth(8)
            .setLabelCol("age")
            .setFeaturesCol("features")

        // Fit the model
        val model = trainer.fit(output)
        model.write.overwrite().save("/home/zaleslaw/models/titanic/gbtreg")

        // Step - 5: Predict with the model
        val rawPredictions = model.transform(output)
        rawPredictions.show(2000)

        // Step - 6: Evaluate prediction
        val evaluator = new RegressionEvaluator()
            .setLabelCol("age")
            .setPredictionCol("prediction")
            .setMetricName("mse")

        // Step - 7: Calculate accuracy
        val mse = evaluator.evaluate(rawPredictions)
        println("MSE = " + mse)

    }
}
