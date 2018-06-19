import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/**
  * The data is described here - https://collegescorecard.ed.gov/data/documentation/
  *
  */
object CollegeScorecard {

  /**
    * Load scorecard data using standard csv mechanism.
    *
    * Get column names from the header - option header is true
    * Infers schema from data in the CSV - option nullValue is "NULL"
    * Converts "NULL" as a null value - option inferSchema is true
    *
    * The returned dataframe should have the following columns (in this order):
    * - UNITID - Integer
    * - OPEID - Integer
    * - INSTNM - String
    * - CITY - String
    * - STABBR - String
    * - COSTT4_A - Integer
    * - DEBT_MDN - Double
    * - C100_4 - Double
    * - C150_4 - Double
    *
    * @param spark the spark session
    * @param path  the path to the csv file(s)
    * @return a dataframe
    */
  def loadScorecardData(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", value = true)
      .option("inferSchema", value = true)
      .option("nullValue", "NULL")
      .load(path)
      .select(
        col("UNITID"),
        col("OPEID").cast("Integer"),
        col("INSTNM"),
        col("CITY"),
        col("STABBR"),
        col("COSTT4_A"),
        col("DEBT_MDN").cast("Double"),
        col("C100_4"),
        col("C150_4")
      )
  }

  /**
    * Create and store a dataframe with the five states with highest average per academic year cost (COSTT4_A)
    * with the following columns (in this order):
    * - STABBR (state abbreviation) - String
    * - COSTT4_A_MEAN (mean academic year cost) - Double
    *
    * The resulting dataframe must be sorted in descending order of academic year cost
    *
    * The dataframe is stored in the configuration.most_expensive_tablename table
    * in the configuration.database database.
    *
    * @param spark         the spark session
    * @param configuration application configuration
    * @return Try[Unit]
    */
  def fiveMostExpensiveStates(spark: SparkSession, configuration: Configuration): Try[Unit] =
    Try {
      spark.sql(s"select * from ${configuration.inputDatabase}.${configuration.collegeScorecardTable}")
        .groupBy("STABBR")
        .agg(mean("COSTT4_A").as("COSTT4_A_MEAN"))
        .orderBy(-col("COSTT4_A_MEAN"))
        .limit(5)
        .write
        .format("ORC")
        .insertInto(s"${configuration.outputDatabase}.${configuration.mostExpensiveTable}")
    }

  /**
    * Create and store a dataframe with five institutions in Texas with highest median student debt (DEBT_MDN)
    * with the following columns (in this order):
    * - UNITID - Integer
    * - OPEID - Integer
    * - INSTNM (institution name) - String
    * - STABBR (state abbreviation) - String
    * - DEBT_MDN (mean academic year cost) - Double
    *
    * The resulting dataframe must be sorted in descending order of median student debt
    *
    * The dataframe is stored in the configuration.highest_debt_tablename table
    * in the configuration.database database.
    *
    * N.B. You'll have to remove any data where median debt is null
    *
    * @param spark         the spark session
    * @param configuration application configuration
    * @return Try[Unit]
    */
  def fiveTexasCollegesWithHighestMedianDebt(spark: SparkSession, configuration: Configuration): Try[Unit] =
    Try {
      spark.sql(s"select * from ${configuration.inputDatabase}.${configuration.collegeScorecardTable}")
        .filter("DEBT_MDN IS NOT NULL AND STABBR == 'TX'")
        .select(
          col("UNITID"),
          col("OPEID"),
          col("INSTNM"),
          col("CITY"),
          col("STABBR"),
          col("DEBT_MDN").cast(DoubleType)
        )
        .orderBy(-col("DEBT_MDN"))
        .limit(5)
        .write
        .format("ORC")
        .insertInto(s"${configuration.outputDatabase}.${configuration.highestDebtTable}")
    }

  /**
    * Crate and store a dataframe with the average and sample standard deviation of the expected time completion rate (C100_4)
    * by city in Texas with the following columns (in this order):
    * - CITY - String
    * - C100_4_MEAN - Double
    * - C100_4_STDDEV - Double
    * - COUNT - Long
    *
    * The resulting dataframe must be sorted in descending order of mean expected time completion rate
    *
    * The dataframe is stored in the configuation.college_scorecard_tablename
    * in the configuration.database database.
    *
    * N.B. You'll have to exclude any record where C100_4 is null and
    * any groupings that have a count of 1 (standard deviation is not defined when the count is
    * less than 2)
    *
    * @param spark         the spark session
    * @param configuration application configuration
    * @return Try[Unit]
    */
  def completionRateStatsInTexasByCity(spark: SparkSession, configuration: Configuration): Try[Unit] =
    Try {
      spark.sql(s"select * from ${configuration.inputDatabase}.${configuration.collegeScorecardTable}")
        .filter("STABBR == 'TX' AND C100_4 IS NOT NULL")
        .groupBy("CITY")
        .agg(
          mean("C100_4").as("C100_4_MEAN"),
          stddev_samp("C100_4").as("C100_4_STDDEV"),
          count("*").as("COUNT")
        )
        .filter("COUNT > 1")
        .orderBy(-col("C100_4_MEAN"))
        .write
        .format("ORC")
        .insertInto(s"${configuration.outputDatabase}.${configuration.completionStatsTable}")
    }

}
