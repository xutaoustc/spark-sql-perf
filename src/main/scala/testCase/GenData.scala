package testCase

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import org.apache.spark.sql.SparkSession

object GenData {
  def main(args: Array[String]): Unit = {
    args match {
      case Array(rootDir,dsdgenDir,scaleFactor,format,useDoubleForDecimal) => {

        val spark = SparkSession
          .builder()
          .appName("GenData")
          .getOrCreate()

        val sqlContext = spark.sqlContext
        val tables = new TPCDSTables(sqlContext,
          dsdgenDir = dsdgenDir,
          scaleFactor = scaleFactor,
          useDoubleForDecimal = useDoubleForDecimal.toBoolean)


        val nonPartitionedTables = Array("call_center", "catalog_page", "customer", "customer_address", "customer_demographics", "date_dim", "household_demographics", "income_band", "item", "promotion", "reason", "ship_mode", "store",  "time_dim", "warehouse", "web_page", "web_site")
        nonPartitionedTables.foreach { t => {
          tables.genData(
            location = rootDir,
            format = format,
            overwrite = true,
            partitionTables = true,
            clusterByPartitionColumns = true,
            filterOutNullPartitionValues = false,
            tableFilter = t,
            numPartitions = 10)
        }}

        val partitionedTables = Array("inventory", "web_returns", "catalog_returns", "store_returns", "web_sales", "catalog_sales", "store_sales")
        partitionedTables.foreach { t => {
          tables.genData(
            location = rootDir,
            format = format,
            overwrite = true,
            partitionTables = true,
            clusterByPartitionColumns = true,
            filterOutNullPartitionValues = false,
            tableFilter = t,
            numPartitions = 10000)
        }}
      }
    }
  }
}
