package Launchers

import org.apache.spark.launcher.SparkLauncher

/**
  * Don't forget install Spark before execution
  */
object SparkSQLRunner {
  def main(args: Array[String]): Unit = {
    System.setProperty("SPARK_HOME", "c:\\programs\\spark\\bin")
    System.setProperty("hadoop.home.dir", "c:\\")
    val handle = new SparkLauncher()
      .setAppResource("spark-core_2.11-1.0.jar")
      .setMainClass("DataFrames.Ex_3_Spark_SQL")
      .setMaster("local")
      .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
      .startApplication();

    Thread.sleep(10000)

    handle.stop()
  }
}
