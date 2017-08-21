package text

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
