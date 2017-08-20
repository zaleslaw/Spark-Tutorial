package supervised.classification.binary

import org.apache.spark.ml.classification.{LinearSVC, NaiveBayes}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

object Ex_3_SVM {
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
      .setOutputCol("features")


    // Step - 2: Transform dataframe to vectorized dataframe
    val output = assembler.transform(indexed5)

    output.show(20, false)

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
  }
}
