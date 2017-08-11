    /*
    SAMPLE-1: Make dataset based on range and extract RDD from it
     */

    val ds = spark.range(100000000)
    println(ds.count)
    println(ds.rdd.count)


    /*
    SAMPLE-2: Make RDD based on Array with reverse order
    */
    val sc = spark.sparkContext

    val r = 1 to 10 toArray
    // Creates RDD with 3 parts
    val ints = sc.parallelize(r.reverse, 3)

    ints.saveAsTextFile("/home/zaleslaw/data/ints") // works for windows well
    val cachedInts = sc.textFile("/home/zaleslaw/data/ints")
      .map(x => x.toInt).cache()

    // Step 1: Transform each number to its square
    val squares = cachedInts
      .map(x => x * x)

    println("--Squares--")
    squares.collect().foreach(println)

    // Step 2: Filter even numbers

    val even = squares.filter(x => x % 2 == 0)

    println("--Even numbers--")
    even.collect().foreach(println)

    // Step 3: print RDD metadata
    even.setName("Even numbers")
    println("Name is " + even.name + " id is " + even.id)
    println(even.toDebugString)

    println("Total multiplication is " + even.reduce((a, b) => a * b))

    // Step 4: Transform to PairRDD make keys 0 for even and 1 for odd numbers and
    val groups = cachedInts.map(x => if (x % 2 == 0) {
      (0, x)
    } else {
      (1, x)
    })

    println("--Groups--")
    println(groups.groupByKey.toDebugString)
    groups.groupByKey.collect.foreach(println)
    println(groups.countByKey)

    // Step 5: different actions
    println("--Different actions--")
    println("First elem is " + cachedInts.first)
    println("Total amount is " + cachedInts.count)
    println("Take 2")
    cachedInts.take(2).foreach(println)
    println("Take ordered 5")
    cachedInts.takeOrdered(5).foreach(println)


2.*Join with PairRDD*

    // A few developers decided to commit something
    // Define pairs <Developer name, amount of commited core lines>
    val codeRows = sc.parallelize(Seq(("Ivan", 240), ("Elena", -15), ("Petr", 39), ("Elena", 290)))

    // Let's calculate sum of code lines by developer
    codeRows.reduceByKey((x, y) => x + y).collect().foreach(println)

    // Or group items to do something else
    codeRows.groupByKey().collect().foreach(println)

    // Don't forget about joins with preffered languages

    val programmerProfiles = sc.parallelize(Seq(("Ivan", "Java"), ("Elena", "Scala"), ("Petr", "Scala")))
    programmerProfiles.saveAsSequenceFile("/home/zaleslaw/data/profiles")

    // Some(classOf[org.apache.hadoop.io.compress.SnappyCodec]) ->  java.lang.RuntimeException: native snappy library not available: this version of libhadoop was built without snappy support.

    // Read and parse data
    val joinResult = sc.sequenceFile("/home/zaleslaw/data/profiles", classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])
      .map { case (x, y) => (x.toString, y.toString) } // transform from Hadoop Text to String
      .join(codeRows)


    joinResult.collect.foreach(println)

    // also we can use special operator to group values from both rdd by key
    // also we sort in DESC order

    programmerProfiles.cogroup(codeRows).sortByKey(false).collect().foreach(println)

    // If required we can get amount of values by each key
    println(joinResult.countByKey)

    // or get all values by specific key
    println(joinResult.lookup("Elena"))

    // codeRows keys only
    codeRows.keys.collect.foreach(println)

    // Print values only
    codeRows.values.collect.foreach(println)


3.*Set theory*

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

4.*Fold*

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

5.*Statistics*

    val anomalInts = sc.parallelize(Seq(1, 1, 2, 2, 3, 150, 1, 2, 3, 2, 2, 1, 1, 1, -100, 2, 2, 3, 4, 1, 2, 3, 4), 3)
    val stats = anomalInts.stats
    val stddev = stats.stdev
    val mean = stats.mean
    println("Stddev is " + stddev + " mean is " + mean)

    val normalInts = anomalInts.filter(x => (math.abs(x - mean) < 3 * stddev))
    normalInts.collect.foreach(println)


6.*Shared variables*


    // Step-1: Let's spread out custom dictionary
    val config = sc.broadcast(("order" -> 3, "filter" -> true))
    println(config.value)


    // Step-2: Let's publish accumulator
    val accum = sc.accumulator(0)

    // ASSERT: "/home/zaleslaw/data/ints" should exists
    val ints = sc.textFile("/home/zaleslaw/data/ints")
    ints.cache()

    ints.foreach(x => accum.add(x.toInt))

    println(accum.value)

    // Step-3: Let's publish accumulator
    // Let's use accumulator in our calculations on executor side

    // sc.textFile("/home/zaleslaw/data/ints").map(_.toInt).filter(e => e > accum.value/2).foreach(println)
    // Of course, you got java.lang.UnsupportedOperationException: Can't read accumulator value in task due to accumulators shared for writing only


    // Step-4: Let's use new version of accumulators
    val accumV2 = new LongAccumulator()
    sc.register(accumV2);
    ints.foreach(x => accumV2.add(x.toLong))


    // If skip this, catch the Caused by: java.lang.UnsupportedOperationException: Accumulator must be registered before send to executor


    if (!accumV2.isZero) {
      println("Sum is " + accumV2.sum)
      println("Avg is " + accumV2.avg)
      println("Count is " + accumV2.count)
      println("Result is " + accumV2.value)
    }

    val accumV2copy = accumV2.copy();
    sc.register(accumV2copy);

    ints.take(5).foreach(x => accumV2copy.add(x.toLong))
    accumV2.merge(accumV2copy)
    println("Merged value " + accumV2.value)

7.*Task: Make custom accumulator -> for exam*

8. *Parse CSV*


    val stateNamesCSV = sc.textFile("/home/zaleslaw/data/StateNames.csv")
    // split / clean data
    val headerAndRows = stateNamesCSV.map(line => line.split(",").map(_.trim))
    // get header
    val header = headerAndRows.first
    // filter out header (eh. just check if the first val matches the first header name)
    val data = headerAndRows.filter(_ (0) != header(0))
    // splits to map (header/value pairs)
    val stateNames = data.map(splits => header.zip(splits).toMap)
    // print top-5
    stateNames.take(5).foreach(println)

    // stateNames.collect // Easy to get java.lang.OutOfMemoryError: GC overhead limit exceeded

    // you should worry about all data transformations to rdd with schema
    stateNames
      .filter(e => e("Name") == "Anna" && e("Count").toInt > 100)
      .take(5)
      .foreach(println)

    // the best way is here: try the DataFrames


****** DATAFRAMES **************
1.*CSV to Parquet/JSON*

    // Step - 1: Extract the schema
    // Read CSV and automatically extract the schema

    val stateNames = spark.read
      .option("header", "true")
      .option("inferSchema", "true") // Id as int, count as int due to one extra pass over the data
      .csv("/home/zaleslaw/data/StateNames.csv")

    stateNames.show
    stateNames.printSchema

    stateNames.write.parquet("/home/zaleslaw/data/stateNames")

    // Step - 2: In reality it can be too expensive and CPU-burst
    // If dataset is quite big, you can infer schema manually
    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

    val nationalNamesSchema = StructType(Array(
      StructField("Id", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("Year", IntegerType, true),
      StructField("Gender", StringType, true),
      StructField("Count", IntegerType, true)))

    val nationalNames = spark.read
      .option("header", "true")
      .schema(nationalNamesSchema)
      .csv("/home/zaleslaw/data/NationalNames.csv")

    nationalNames.show
    nationalNames.printSchema
    nationalNames.write.json("/home/zaleslaw/data/nationalNames")
    // nationalNames.write.orc("/home/zaleslaw/data/nationalNames")
    // this is available only with HiveContext in opposite you will get an exception
    // Exception in thread "main" org.apache.spark.sql.AnalysisException: The ORC data source must be used with Hive support enabled;

    nationalNames.cache()

    // Step - 3: Simple dataframe operations

    // filter & select & orderBy
    nationalNames
      .where("Gender == 'M'")
      .select("Name", "Year", "Count")
      .orderBy("Name", "Year")
      .show(100)


    // Registered births by year in US since 1880
    nationalNames
      .groupBy("Year")
      .sum("Count").as("Sum")
      .orderBy("Year")
      .show(200)


2.*Most popular girl*

    // ASSERT: Files should exists
    val stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")

    // Step - 3: Task - Get most popular girl name in each state in 1945
    val filteredStateNames = stateNames
      .where("Year=1945 and Gender='F'")
      .select("Name", "State", "Count")

    filteredStateNames.cache

    import spark.implicits._ // very important import

    filteredStateNames.orderBy($"state".desc, $"Count".desc).show

    import org.apache.spark.sql.functions._ // to support different functions

    val stateAndCount = filteredStateNames
      .groupBy("state")
      .agg(max("Count") as "max")

    stateAndCount.show

    // Self-join, of course
    val stateAndName = filteredStateNames
      .join(stateAndCount,
        stateAndCount.col("max").equalTo(filteredStateNames.col("Count"))
          and
          stateAndCount.col("state").equalTo(filteredStateNames.col("state")))
      .select(filteredStateNames.col("state"), $"Name".alias("name")) // should choose only String names or $Columns
      .orderBy($"state".desc, $"Count".desc)

    stateAndName.printSchema
    stateAndName.show
    stateAndName.explain
    stateAndName.explain(extended = true)

3.*Spark SQL*

    // ASSERT: Files should exists
    val stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames")

    stateNames.createOrReplaceTempView("stateNames")


    // Step-1: Get full list of boy names
    spark.sql("SELECT DISTINCT Name FROM stateNames WHERE Gender = 'M' ORDER BY Name").show(100)

    // Step-2: Get proportion of state NY births in total births
    val nationalNames = spark.read.json("/home/zaleslaw/data/nationalNames")

    nationalNames.createOrReplaceTempView("nationalNames")

    val result = spark.sql("SELECT nyYear as year, stateBirths/usBirths as proportion, stateBirths, usBirths FROM (SELECT year as nyYear, SUM(count) as stateBirths FROM stateNames WHERE state = 'NY' GROUP BY year ORDER BY year) as NY" +
      " JOIN (SELECT year as usYear, SUM(count) as usBirths FROM nationalNames GROUP BY year ORDER BY year) as US ON nyYear = usYear")

    result.show(150)
    result.explain(extended = true)


4.*Datasets*

    // Step - 1: Define schema in code with case classes


    import spark.implicits._
    // ASSERT: Files should exists
    val stateNames = spark.read.parquet("/home/zaleslaw/data/stateNames").as[stateNamesRow]
    stateNames.show

    val nationalNames = spark.read.json("/home/zaleslaw/data/nationalNames").as[nationalNamesRow]
    nationalNames.show

    // Step - 2: Some typed operations

    stateNames.cache()

    val result = stateNames
      .map(x => (x.name, x.state, x.count))
      .filter(e => e._2 == "NY")
      .groupBy("_1")
      .sum("_3")
      .sort($"sum(_3)".desc)

    result.show
    result.explain(extended = true)

  }

  // Move it outside main method in other case you will get Unable to find encoder for type stored in a Dataset.  Primitive types (Int, String, etc) and Product types (case classes) are supported by importing sqlContext.implicits._  Support for serializing other types will be added in future releases.
  case class stateNamesRow(id: Long, name: String, year: Long, gender: String, state: String, count: Int)

  case class nationalNamesRow(id: Long, name: String, year: Long, gender: String, count: Long)

  // I made all ints in both cases to long due to encoding troubles to skip custom encoders definition
  // before that I had gotten an error: " Exception in thread "main" org.apache.spark.sql.AnalysisException: Cannot up cast `count` from bigint to int as it may truncate
  /*  The type path of the target object is:
      - field (class: "scala.Int", name: "count")
    - root class: "DataFrames.Ex_4_Datasets.nationalNamesRow"
    You can either add an explicit cast to the input data or choose a higher precision type of the field in the target object;*/