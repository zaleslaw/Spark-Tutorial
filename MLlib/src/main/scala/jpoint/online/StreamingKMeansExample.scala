package jpoint.online

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Estimate clusters on one stream of data and make predictions
  * on another stream, where the data streams arrive as text files
  * into two different directories.
  *
  * The rows of the training text files must be vector data in the form
  * `[x1,x2,x3,...,xn]`
  * Where n is the number of dimensions.
  *
  * The rows of the test text files must be labeled data in the form
  * `(y,[x1,x2,x3,...,xn])`
  * Where y is some identifier. n must be the same for train and test.
  *
  * Usage:
  *   StreamingKMeansExample <trainingDir> <testDir> <batchDuration> <numClusters> <numDimensions>
  *
  * To run on your local machine using the two directories `trainingDir` and `testDir`,
  * with updates every 5 seconds, 2 dimensions per data point, and 3 clusters, call:
  *    $ bin/run-example mllib.StreamingKMeansExample trainingDir testDir 5 3 2
  *
  * As you add text files to `trainingDir` the clusters will continuously update.
  * Anytime you add text files to `testDir`, you'll see predicted labels using the current model.
  *
  */


object StreamingKMeansExample {

  def main(args: Array[String]) {

      System.setProperty("hadoop.home.dir", "c:\\")

      val trainingDir = "C:\\home\\zaleslaw\\streaming\\kmeansTestDir"
      val testDir = "C:\\home\\zaleslaw\\streaming\\kmeansTestDir"
      val batchDuration = 10
      val numClusters = 4
      val numDimensions = 4



    // $example on$
    val conf = new SparkConf().setAppName("StreamingKMeansExampleee").setMaster("local[2]")
      val ssc = new StreamingContext(conf, Seconds(batchDuration))

    val trainingData = ssc.textFileStream(trainingDir)//.map(Vectors.parse)
    val testData = ssc.textFileStream(testDir)//.map(LabeledPoint.parse)

      trainingData.foreachRDD(rdd => println (rdd.count()))

      testData.foreachRDD(rdd =>
          rdd.foreach(
              x => print(x)
          )
      )
    val model = new StreamingKMeans()
      .setK(numClusters)
      .setDecayFactor(1.0)
      .setRandomCenters(numDimensions, 0.0)

    //model.trainOn(trainingData)
   // model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print(12)

    ssc.start()
    ssc.awaitTermination()
    // $example off$
  }
}
