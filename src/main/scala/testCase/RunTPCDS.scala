package testCase

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.spark.sql.SparkSession

object RunTPCDS {
  def main(args: Array[String]): Unit = {

    args match {
      case Array(rootDir,dsdgenDir,scaleFactor,format,resultLocation) =>{
        val spark = SparkSession
          .builder()
          .appName("TPCDS_TEST")
          .getOrCreate()

        val sqlContext = spark.sqlContext
        val tables = new TPCDSTables(sqlContext,
          dsdgenDir = dsdgenDir,
          scaleFactor = scaleFactor)

        tables.createTemporaryTables(rootDir, format)


        val tpcds = new TPCDS(sqlContext)
        val iterations = 1
        val queries = tpcds.tpcds2_4Queries

        val experiment = tpcds.runExperiment(
          queries,
          iterations = iterations,
          resultLocation = resultLocation,
          forkThread = true)

        val timeout = Int.MaxValue
        experiment.waitForFinish(timeout)

        println("finished")
      }
      case _ => println("argument not match")

    }
  }
}
