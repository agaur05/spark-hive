import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

object TestHive
  extends TestHiveContext(
    new SparkContext(
      "local[*]",
      "TestSQLContext",
      new SparkConf()
        .set("spark.sql.warehouse.dir", {
          val warehouseDir = java.io.File.createTempFile("warehouse", "")
          warehouseDir.delete()
          warehouseDir.toURI.getPath
        })
        .set("spark.ui.enabled", "false")
        .set("spark.driver.host", "localhost")),
    false)

trait SparkEnvironment extends BeforeAndAfterAll { this: Suite =>
  def withSpark(testCode: (SparkSession) => Any): Unit = {
    try {
      testCode(TestHive.sparkSession)
    } finally {
      // Reset hive context before each test case
      TestHive.reset()
      TestHive.setCacheTables(false)
      TestHive.sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    }
  }

  override def beforeAll() {
    super.beforeAll()
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  override def afterAll() {
    super.afterAll()
    TestHive.sparkSession.stop()
  }

  def createDatabases(databases: Seq[String]): Unit = {
    databases.foreach { database =>
      TestHive.sparkSession.sql(s"create database $database").collect()
    }
  }

  def saveAsTableFromCSV(fileName: String, header: Boolean, inferSchema: Boolean, tableName: String, partitionSpec: Seq[String]) {
    loadDataFrameFromCSV(fileName, header, inferSchema).write.format("ORC").partitionBy(partitionSpec: _*).saveAsTable(tableName)
  }

  def saveAsTableFromCSV(fileName: String, header: Boolean, inferSchema: Boolean, tableName: String) {
    loadDataFrameFromCSV(fileName, header, inferSchema).write.format("ORC").saveAsTable(tableName)
  }

  def loadDataFrameFromCSV(path: String, header: Boolean = true, inferSchema: Boolean = true): DataFrame = {
    TestHive.sparkSession.read.format("csv").option("header", header).option("inferSchema", inferSchema).load(path)
  }

}
