package RDD

import org.apache.spark.sql.SparkSession

object Ex_4_Fold {
  def main(args: Array[String]): Unit = {

    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\")

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("Fold")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext


    // Step-1: Find greatest contributor

    val codeRows = sc.makeRDD(List(("Ivan", 240), ("Elena", -15), ("Petr", 39), ("Elena", 290)))

    val zeroCoder = ("zeroCoder", 0);

    val greatContributor = codeRows.fold(zeroCoder)(
      (acc, coder) => {
        if (acc._2 < Math.abs(coder._2)) coder else acc
      })

    println("Developer with maximum contribution is " + greatContributor)

    // Step-2:

    val codeRowsBySkill = sc.makeRDD(List(("Java", ("Ivan", 240)), ("Java", ("Elena", -15)), ("PHP", ("Petr", 39)), ("PHP", ("Elena", 290))))

    val maxBySkill = codeRowsBySkill.foldByKey(zeroCoder)(
      (acc, coder) => {
        if (acc._2 > Math.abs(coder._2)) acc else coder
      })

    println("Greatest contributor by skill are " + maxBySkill.collect().toList)
  }
}

