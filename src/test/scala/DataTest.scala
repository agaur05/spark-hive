import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, FlatSpec}

class DataTest extends FlatSpec with SparkEnvironment with BeforeAndAfter {

  // Shared dataframe across all test cases
  // We cache it to make access faster
  private val scoreCardFile = getClass.getResource("/MERGED2015_16_PP.csv.gz").getFile
  private val scoreCardDF: DataFrame = CollegeScorecard.loadScorecardData(
    TestHive.sparkSession,
    scoreCardFile
  ).cache()

  // Shared config across all test cases
  private val configuration = Configuration(
    inputDatabase = "global_temp", // Database created by Spark for Global Temp Views
    collegeScorecardTable = "college_scorecard",
    outputDatabase = "mydb",
    mostExpensiveTable = "most_expensive",
    highestDebtTable = "highest_debt",
    completionStatsTable = "completion_rate"
  )

  // Make sure the Hive tables are re-created before each test case
  before {

    // Create database
    createDatabases(Seq(configuration.outputDatabase))

    // Create input table
    // We use a global temp view to save time, it's faster than storing the data before every test
    // this way we're accessing the cached dataframe from earlier
    scoreCardDF.createOrReplaceGlobalTempView(s"${configuration.collegeScorecardTable}")

    // Create output tables
    TestHive.sparkSession.sql(
      s"""
         |CREATE TABLE ${configuration.outputDatabase}.${configuration.mostExpensiveTable} (
         |  STABBR STRING,
         |  COSTT4_A_MEAN DOUBLE
         |)
         |STORED AS ORC
       """.stripMargin).collect
    TestHive.sparkSession.sql(
      s"""
         |CREATE TABLE ${configuration.outputDatabase}.${configuration.highestDebtTable} (
         |  UNITID INTEGER,
         |  OPEID INTEGER,
         |  INSTNM STRING,
         |  CITY STRING,
         |  STABBR STRING,
         |  DEBT_MDN DOUBLE
         |)
         |STORED AS ORC
       """.stripMargin).collect
    TestHive.sparkSession.sql(
      s"""
         |CREATE TABLE ${configuration.outputDatabase}.${configuration.completionStatsTable} (
         |  CITY STRING,
         |  C100_4_MEAN DOUBLE,
         |  C100_4_STDDEV DOUBLE,
         |  COUNT BIGINT
         |)
         |STORED AS ORC
       """.stripMargin).collect
  }

  "the college scorecard data" must "be loadable and  have the correct schema" in withSpark { spark =>
    // Make sure we have the correct columns for the College Scorecard data
    val scoreCardSchema = StructType(
      Array(
        StructField("UNITID", IntegerType),
        StructField("OPEID", IntegerType),
        StructField("INSTNM", StringType),
        StructField("CITY", StringType),
        StructField("STABBR", StringType),
        StructField("COSTT4_A", IntegerType),
        StructField("DEBT_MDN", DoubleType),
        StructField("C100_4", DoubleType),
        StructField("C150_4", DoubleType)
      )
    )
    val scoreCardDF = spark.sql(s"select * from ${configuration.inputDatabase}.${configuration.collegeScorecardTable}")
    assert(scoreCardDF.count == 7593)
    assert(scoreCardDF.schema == scoreCardSchema)
  }

  "five most expensive states" must "store a dataframe with the correct columns and types" in withSpark { spark =>
    val schema = StructType(
      Array(
        StructField("STABBR", StringType),
        StructField("COSTT4_A_MEAN", DoubleType)
      )
    )
    val result = CollegeScorecard.fiveMostExpensiveStates(spark, configuration)
    assert(result.isSuccess, if (result.isFailure) result.failed.get)
    val df = spark.sql(s"select * from ${configuration.outputDatabase}.${configuration.mostExpensiveTable}")
    assert(df.schema == schema)
    assert(df.count == 5)
  }

  "five most expensive states" must "store the correct data and number of rows" in withSpark { spark =>
    val rows = Seq(
      ("RI", 41404.166666666664),
      ("VT", 39753.944444444445),
      ("MA", 39037.59223300971),
      ("DC", 37975.61538461538),
      ("PA", 32211.23474178404)
    )
    val expectedDF = spark.createDataFrame(rows)
      .toDF("STABBR", "COSTT4_A_MEAN")
      .withColumn("COSTT4_A_MEAN", round(col("COSTT4_A_MEAN"), 2))
    val result = CollegeScorecard.fiveMostExpensiveStates(spark, configuration)
    assert(result.isSuccess, if (result.isFailure) result.failed.get)
    val df = spark.sql(s"select * from ${configuration.outputDatabase}.${configuration.mostExpensiveTable}")
      .withColumn("COSTT4_A_MEAN", round(col("COSTT4_A_MEAN"), 2))
    val diff = df.union(expectedDF).except(df.intersect(expectedDF))
    assert(diff.count == 0)
  }

  "five institutions with highest median student debt in texas" must "store a dataframe with the correct columns and types" in withSpark { spark =>
    val schema = StructType(
      Array(
        StructField("UNITID", IntegerType),
        StructField("OPEID", IntegerType),
        StructField("INSTNM", StringType),
        StructField("CITY", StringType),
        StructField("STABBR", StringType),
        StructField("DEBT_MDN", DoubleType)
      )
    )
    val result = CollegeScorecard.fiveTexasCollegesWithHighestMedianDebt(spark, configuration)
    assert(result.isSuccess, if (result.isFailure) result.failed.get)
    val df = spark.sql(s"select * from ${configuration.outputDatabase}.${configuration.highestDebtTable}")
    assert(df.schema == schema)
  }

  "five institutions with highest median student debt in texas" must "store the correct data and number of rows" in withSpark { spark =>
    val rows = Seq(
      (477039, 3698304, "West Coast University-Dallas", "Dallas", "TX", 26500.0),
      (228149, 362300, "St. Mary's University", "San Antonio", "TX", 24000.0),
      (228343, 362000, "Southwestern University", "Georgetown", "TX", 23250.0),
      (222983, 354300, "Austin College", "Sherman", "TX", 22250.0),
      (227845, 362100, "Saint Edward's University", "Austin", "TX", 21500.0)
    )
    val expectedDF = spark.createDataFrame(rows)
      .toDF("UNITID", "OPEID", "INSTNM", "CITY", "STABBR", "DEBT_MDN")
    val result = CollegeScorecard.fiveTexasCollegesWithHighestMedianDebt(spark, configuration)
    assert(result.isSuccess, if (result.isFailure) result.failed.get)
    val df = spark.sql(s"select * from ${configuration.outputDatabase}.${configuration.highestDebtTable}")
    val diff = df.union(expectedDF).except(df.intersect(expectedDF))
    assert(diff.count == 0)
  }

  "completion stats in texas by city" must "store a dataframe with the correct columns and types" in withSpark { spark =>
    val schema = StructType(
      Array(
        StructField("CITY", StringType),
        StructField("C100_4_MEAN", DoubleType),
        StructField("C100_4_STDDEV", DoubleType),
        StructField("COUNT", LongType)
      )
    )
    val result = CollegeScorecard.completionRateStatsInTexasByCity(spark, configuration)
    assert(result.isSuccess, if (result.isFailure) result.failed.get)
    val df = spark.sql(s"select * from ${configuration.outputDatabase}.${configuration.completionStatsTable}")
    assert(df.count == 12)
    assert(df.schema == schema)
  }

  "completion stats in texas by city" must "store the correct data and number of rows" in withSpark { spark =>
    val rows = Seq(
      ("Irving",0.38475,0.39576766543011066,2),
      ("Fort Worth",0.38149999999999995,0.2921765219862814,2),
      ("Abilene",0.34053333333333335,0.07716516917193489,3),
      ("San Antonio",0.33025,0.1923298803024191,8),
      ("Lubbock",0.30725,0.06116473657263634,2),
      ("Austin",0.27396666666666664,0.2059173296900158,6),
      ("Dallas",0.2526333333333333,0.25985945175549546,6),
      ("Denton",0.2311,0.05600285706997456,2),
      ("Houston",0.21688999999999997,0.24472583049427193,10),
      ("Marshall",0.19315000000000002,0.20838436841567556,2),
      ("Arlington",0.1557,0.13758593678134404,3),
      ("Tyler",0.15045,0.13413815639108806,2)
    )
    val expectedDF = spark.createDataFrame(rows)
      .toDF("CITY", "C100_4_MEAN", "C100_4_STDDEV", "COUNT")
      .withColumn("C100_4_MEAN", round(col("C100_4_MEAN"), 2))
      .withColumn("C100_4_STDDEV", round(col("C100_4_STDDEV"), 2))
    val result = CollegeScorecard.completionRateStatsInTexasByCity(spark, configuration)
    assert(result.isSuccess, if (result.isFailure) result.failed.get)
    val df = spark.sql(s"select * from ${configuration.outputDatabase}.${configuration.completionStatsTable}")
      .withColumn("C100_4_MEAN", round(col("C100_4_MEAN"), 2))
      .withColumn("C100_4_STDDEV", round(col("C100_4_STDDEV"), 2))
    val diff = df.union(expectedDF).except(df.intersect(expectedDF))
    assert(diff.count == 0)
  }

}
