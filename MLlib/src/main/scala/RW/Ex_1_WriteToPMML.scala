package RW

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

/**
  * How to share trained model in Spark?
  * The best approach - is serialization and sharing through the file.
  */
object Ex_1_WriteToPMML{
    def main(args: Array[String]): Unit = {

        //For windows only: don't forget to put winutils.exe to c:/bin folder
        System.setProperty("hadoop.home.dir", "c:\\")

        val spark = SparkSession.builder
            .master("local")
            .appName("Spark_SQL")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val iris = spark.read
            .option("inferSchema", "true")
            .option("delimiter", "\t")
            .csv("/home/zaleslaw/data/two_classed_iris.csv")

        //iris.show()
        iris.rdd.foreach(println)

        import org.apache.spark.mllib.linalg.Vectors

        val Array(training, test) = iris.randomSplit(Array(0.7, 0.3), seed = 12345)
        val trainingRDD = training.rdd.map(row => LabeledPoint(row.getInt(0), Vectors.dense(row.getDouble(1), row.getDouble(2), row.getDouble(3), row.getDouble(4))))
        val testRDD = test.rdd.map(row => LabeledPoint(row.getInt(0), Vectors.dense(row.getDouble(1), row.getDouble(2), row.getDouble(3), row.getDouble(4))))


        val oldLogRegModel = new LogisticRegressionWithLBFGS()
            .setNumClasses(2)
            .run(trainingRDD)


        val predictionAndLabels = testRDD.map { case LabeledPoint(label, features) =>
            val prediction = oldLogRegModel.predict(features)
            (prediction, label)
        }

        // Get evaluation metrics.
        val metrics = new MulticlassMetrics(predictionAndLabels)
        val oldaccuracy = metrics.accuracy
        println("Accuracy " + oldaccuracy)

        println(metrics.confusionMatrix)

        oldLogRegModel.toPMML("/home/zaleslaw/data/iris")

       /* // Step - 1: Make Vectors from dataframe columns using special Vector Assmebler

        val assembler = new VectorAssembler()
            .setInputCols(Array("_c1", "_c2", "_c3", "_c4"))
            .setOutputCol("features")

        // Step - 2: Transform dataframe to vectorized dataframe
        val output = assembler.transform(training)


        val trainer = new LogisticRegression()
            .setMaxIter(20)
            .setRegParam(0.1)
            .setElasticNetParam(0.3)
            .setLabelCol("_c0")

        val logRegModel = trainer.fit(output)

        println(s"Coefficients: ${logRegModel.coefficients} Intercept: ${logRegModel.intercept}")



        val predictions = logRegModel.transform(assembler.transform(test))

        predictions.show()

        val evaluator = new BinaryClassificationEvaluator()
            .setLabelCol("_c0")
            .setRawPredictionCol("prediction")

        val accuracy = evaluator.evaluate(predictions)
        println("Test set accuracy = " + accuracy)*/



    }
}
