package RDD

import org.apache.spark.sql.SparkSession

object Ex_3_Set_theory {
  def main(args: Array[String]): Unit = {

    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Set_theory")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    // Set Theory in Spark
    val jvmLanguages = sc.parallelize(List("Scala", "Java", "Groovy", "Kotlin", "Ceylon"))
    val functionalLanguages = sc.parallelize(List("Scala", "Kotlin", "JavaScript", "Haskell"))
    val webLanguages = sc.parallelize(List("PHP", "Ruby", "Perl", "PHP", "JavaScript"))

    println("----Distinct----")
    val distinctLangs = webLanguages.union(jvmLanguages).distinct()
    println(distinctLangs.toDebugString)
    distinctLangs.collect.foreach(println)

    println("----Intersection----")
    val intersection = jvmLanguages.intersection(functionalLanguages)
    println(intersection.toDebugString)
    intersection.collect.foreach(println)

    println("----Substract----")
    val substraction = webLanguages.distinct.subtract(functionalLanguages)
    println(substraction.toDebugString)
    substraction.collect.foreach(println)

    println("----Cartesian----")
    val cartestian = webLanguages.distinct.cartesian(jvmLanguages)
    println(cartestian.toDebugString)
    cartestian.collect.foreach(println)


  }
}

