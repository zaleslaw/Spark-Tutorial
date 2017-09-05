1.*Stats*

package stats

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics


/**
  * Funny correlation between CO2 concentration and amount of Emma girls in USA
  */

object Ex_1_Correlation_CO2_Emmas {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark_SQL")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // ASSERT: Files should exists
    val nationalNames = spark.read.json("/home/zaleslaw/data/nationalNames")

    import spark.implicits._

    val emmaByYear = nationalNames
      .where("Name = 'Emma' and Gender = 'F'") // because there are boys with Emma as a name
      .select($"Year", $"Count")

    val co2 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/home/zaleslaw/data/co2.csv")


    val dataset = emmaByYear
      .join(co2, "year")
      .select($"count", $"data_mean_global".as("concentration"))


    // Pearson
    println(dataset.stat.corr("count", "concentration"))


    // for Spearman correlation we need to use ML algebra with Vectors

    // Step - 1: Make Vectors from dataframe columns using special Vector Assmebler

    val assembler = new VectorAssembler()
      .setInputCols(Array("count", "concentration"))
      .setOutputCol("features")


    val output = assembler.transform(dataset).select("features")

    output.show()

    // Step - 2: Calculate Correlation Matrix with two different methods
    val Row(coeff1: Matrix) = Correlation.corr(output, "features").head
    println("Pearson correlation matrix:\n" + coeff1.toString)

    val Row(coeff2: Matrix) = Correlation.corr(output, "features", "spearman").head
    println("Spearman correlation matrix:\n" + coeff2.toString)


    // Step - 3: Print out another stats
    // Convert initial data to mllib.Vector (instead of new ml.Vector) to use the power of old API
    val vectors = dataset.rdd
      .map (row => Vectors.dense(row.getAs[Long]("count").toDouble, row.getAs[Double]("concentration")))


    val summary = Statistics.colStats(vectors)
    println("Statistics")
    println(summary.count)
    println(summary.max.toArray.deep.mkString(" "))
    println(summary.min.toArray.deep.mkString(" "))
    println(summary.mean.toArray.deep.mkString(" "))
    println(summary.normL1.toArray.deep.mkString(" "))
    println(summary.variance.toArray.deep.mkString(" "))
  }
}

2.*Clustering*

package unsupervised.clustering

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


/**
  * Try to find clusters in small dataset and compare it with real classes
  */
object Ex_1_KMeans {
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
      .option("header", "true")
      .csv("/home/zaleslaw/data/animals.csv")

    animals.show()

    //Exception in thread "main" java.lang.IllegalArgumentException: Field "features" does not exist.
    // don't forget transform all your columns with VectorAssembler to 'features'

    // Step - 1: Make Vectors from dataframe columns using special Vector Assmebler

    val assembler = new VectorAssembler()
      .setInputCols(Array("hair", "feathers", "eggs", "milk", "airborne", "aquatic", "predator", "predator", "toothed",
        "backbone", "breathes", "venomous", "fins", "legs", "tail", "domestic", "catsize"))
      .setOutputCol("features")


    // Step - 2: Transform dataframe to vectorized dataframe
    val output = assembler.transform(animals).select("features", "name", "type")

    // Step - 3: Train model
    val kmeans = new KMeans()
      .setK(7) // possible number of clusters: change it from 2 to 10, optimize parameter
      .setSeed(1L)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")

    val model = kmeans.fit(output)

    // Step - 4: Print out the
    // sum of squared distances of points to their nearest center
    val WSSSE = model.computeCost(output)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Step - 5: Print out clusters
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    println("Real clusters and predicted clusters")
    val predictions = model.summary.predictions
    predictions.show(200)


    // Step - 6: Print out predicted and real classes
    import org.apache.spark.sql.functions._

    println("Predicted classes")
    predictions
      .select("name", "cluster")
      .groupBy("cluster")
      .agg(collect_list("name")).show(predictions.count().toInt, false)

    println("Real classes")
    output
      .select("name", "type")
      .groupBy("type")
      .agg(collect_list("name")).show(predictions.count().toInt, false)


  }
}


    // Step - 3: Train model
    val kmeans = new BisectingKMeans()
      .setK(7) // possible number of clusters: change it from 2 to 10, optimize parameter
      .setSeed(1L)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")

    val model = kmeans.fit(output)

    // Step - 4: Print out the
    // sum of squared distances of points to their nearest center
    val WSSSE = model.computeCost(output)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Step - 5: Print out clusters
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    println("Real clusters and predicted clusters")
    val predictions = model.summary.predictions
    predictions.show(200)


        // Step - 3: Train model
        val kmeans = new GaussianMixture()
          .setK(7) // possible number of clusters: change it from 2 to 10, optimize parameter
          .setSeed(1L)
          .setFeaturesCol("features")
          .setPredictionCol("cluster")
          .setProbabilityCol("prob")

        val model = kmeans.fit(output)

        // Step - 4: Print out the
        for (i <- 0 until model.getK) {
          println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
            s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
        }

        println("Real clusters and predicted clusters")
        val predictions = model.summary.predictions
        predictions.show(200, false)


3.*Binary classification*

import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

object Ex_1_LogisticRegression {
  def main(args: Array[String]): Unit = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("Logistic Regression")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    val mushrooms = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/home/zaleslaw/data/mushrooms.csv")

    mushrooms.show()

    // Step - 1: Make Vectors from dataframe columns using special Vector Assmebler



    import org.apache.spark.ml.feature.StringIndexer



    val classIndexer = new StringIndexer()
      .setInputCol("class")
      .setOutputCol("class_i")
      .fit(mushrooms)

    val classed = classIndexer.transform(mushrooms)


    val indexer1 = new StringIndexer()
      .setInputCol("cap-shape")
      .setOutputCol("cap-shape_i")
      .fit(classed)

    val indexed = indexer1.transform(classed)

    val indexer2 = new StringIndexer()
      .setInputCol("cap-surface")
      .setOutputCol("cap-surface_i")
      .fit(indexed)

    val indexed2 = indexer2.transform(indexed)

    val indexer3 = new StringIndexer()
      .setInputCol("cap-color")
      .setOutputCol("cap-color_i")
      .fit(indexed2)

    val indexed3 = indexer3.transform(indexed2)

    val indexer4 = new StringIndexer()
      .setInputCol("bruises")
      .setOutputCol("bruises_i")
      .fit(indexed3)

    val indexed4 = indexer4.transform(indexed3)

    val indexer5 = new StringIndexer()
      .setInputCol("odor")
      .setOutputCol("odor_i")
      .fit(indexed4)

    val indexed5 = indexer5.transform(indexed4)


    val assembler = new VectorAssembler()
      .setInputCols(Array("cap-shape_i", "cap-surface_i", "cap-color_i", "bruises_i", "odor_i"))

      /*  "odor", "gill-attachment", "gill-spacing", "gill-size", "gill-color",
       "stalk-shape", "stalk-root", "stalk-surface-above-ring", "stalk-surface-below-ring", "stalk-color-above-ring", "stalk-color-below-ring",
       "veil-type", "veil-color", "ring-number", "ring-type", "spore-print-color", "population", "habitat"))*/
      .setOutputCol("features")


    // Step - 2: Transform dataframe to vectorized dataframe
    val output = assembler.transform(indexed5)

    output.show(20, false)

    val lr = new LogisticRegression()
      .setMaxIter(20)
      .setRegParam(0.1)
      .setElasticNetParam(0.3)
      .setLabelCol("class_i")

    val lrModel = lr.fit(output)



    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Extract the summary from the returned LogisticRegressionModel instance trained in the earlier
    // example
    val trainingSummary = lrModel.summary

    // Obtain the objective per iteration.
    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss => println(loss))

    // Obtain the metrics useful to judge performance on test data.
    // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a
    // binary classification problem.
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
    val roc = binarySummary.roc
    roc.show()
    println(s"areaUnderROC: ${binarySummary.areaUnderROC}")

    import spark.implicits._
    import org.apache.spark.sql.functions._
    // Set the model threshold to maximize F-Measure
    val fMeasure = binarySummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
      .select("threshold").head().getDouble(0)
    lrModel.setThreshold(bestThreshold)



    val predictions = lrModel.transform(output.sample(false, 0.1))
    predictions.show

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("class_i")
      .setRawPredictionCol("prediction")
    val accuracy = evaluator.evaluate(predictions)
    println("Test set accuracy = " + accuracy)

  }

}


    val nb = new NaiveBayes()
      .setLabelCol("class_i")

    val nbModel = nb.fit(output)



    println(s"Theta: ${nbModel.theta.toString()}")


    val predictions = nbModel.transform(output.sample(false, 0.1))
    predictions.show

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("class_i")
      .setRawPredictionCol("prediction")
    val accuracy = evaluator.evaluate(predictions)
    println("Test set accuracy = " + accuracy)



     val lsvc = new LinearSVC()
          .setMaxIter(10)
          .setRegParam(0.1)
          .setLabelCol("class_i")

        val lsvcModel = lsvc.fit(output)



        println(s"Coefficients: ${lsvcModel.coefficients} Intercept: ${lsvcModel.intercept}")


        val predictions = lsvcModel.transform(output.sample(false, 0.1))
        predictions.show

        val evaluator = new BinaryClassificationEvaluator()
          .setLabelCol("class_i")
          .setRawPredictionCol("prediction")
        val accuracy = evaluator.evaluate(predictions)
        println("Test set accuracy = " + accuracy)

4.*Text*

import org.apache.spark.sql.SparkSession

object TextSample {
  def main(args: Array[String]): Unit = {

    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("Text")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.classification.LogisticRegression
    import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
    // Prepare training documents from a list of (id, text, label) tuples.
    val training = spark.sqlContext.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark top pop", 1.0),
      (3L, "spark flink kafka", 1.0),
      (4L, "spark shlep stop", 1.0),
      (5L, "hadoop mapreduce", 0.0)))
      .toDF("id", "text", "label")
    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
    val lr = new LogisticRegression().setMaxIter(20).setRegParam(0.01)
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
    // Fit the pipeline to training documents.

    val model = pipeline.fit(training)

    // Prepare test documents, which are unlabeled (id, text) tuples.

    val test = spark.sqlContext.createDataFrame(Seq( (6L, "spark kafka"),
      (7L, "apacvhe"),
      (8L, "mapreduce spark"),
      (9L, "apache hadoop")))
      .toDF("id", "text")
    // Make predictions on test documents.
    model.transform(test).select("id", "text", "probability", "prediction")
      .show()

  }
}
