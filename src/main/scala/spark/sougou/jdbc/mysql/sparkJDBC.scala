package spark.sougou.jdbc.mysql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/7/4 17:18
 * @Version: 1.0
 * @Function:
 */
object sparkJDBC {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SparkReadTxtAndInsertIntoMySQL")
      .master("local[*]")
      .getOrCreate()

    // Read the txt file as a DataFrame
    val dataFrame: DataFrame = spark.read.text("data/datafile.txt")

    // Define the MySQL connection properties
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "root")

    // Write the DataFrame to MySQL
    dataFrame.write
      .mode(SaveMode.Append)
      .jdbc("jdbc:mysql://localhost:3306/test", "inserttest", connectionProperties)

    // Stop the SparkSession
    spark.stop()
  }
}
