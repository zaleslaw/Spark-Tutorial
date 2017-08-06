package DataFrames

/**
  * val people: RDD[Person] = ...
  * *
  *people.write.parquet("people.parquet")
  * *
  * val parquetFile = sqlContext.read.parquet("people.parquet")
  * *
  *parquetFile.registerTempTable("parquetFile")
  * *
  * val teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
  * *
  *teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
  *
  */
object WorkingWithParquet {

}
