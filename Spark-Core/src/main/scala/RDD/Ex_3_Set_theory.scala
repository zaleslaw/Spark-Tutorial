package RDD

import org.apache.spark.sql.SparkSession

object Ex_3_Set_theory {
  def main(args: Array[String]): Unit = {

    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("RDD_Intro")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    // Set Theory in Spark
    val jvmLanguages = sc.parallelize(List("Scala", "Java", "Groovy", "Kotlin", "Ceylon"))
    val functionalLanguages = sc.parallelize(List("Scala", "Kotlin", "JavaScript", "Haskell"))
    val webLanguages = sc.parallelize(List("PHP", "Ruby", "Perl", "PHP", "JavaScript"))

    val result = webLanguages.distinct.union(jvmLanguages)
    println(result.toDebugString)
    println("----Distinct----")
    result.collect.foreach(println)

    println("----Intersection----")
    jvmLanguages.intersection(functionalLanguages).collect.foreach(println)
    println("----Subtract----")
    webLanguages.subtract(functionalLanguages).collect.foreach(println)
    println("----Cartesian----")
    webLanguages.cartesian(jvmLanguages).collect.foreach(println)


  }
}

