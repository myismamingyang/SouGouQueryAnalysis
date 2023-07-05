package spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/7/5 14:52
 * @Version: 1.0
 * @Function:
 */
object wordJDBC {
  case class inserttest(id:Int,create_date_time:String,session_id:String)
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //TODO 1.加载数据
    val lines: RDD[String] = sc.textFile("data/datafile.txt")
    val personRDD: RDD[inserttest] = lines.map(line => {
      val arr: Array[String] = line.split(" ")
      inserttest(arr(0).toInt, arr(1), arr(2))
    })
    import spark.implicits._
    val personDF: DataFrame = personRDD.toDF()

    personDF.printSchema()
    personDF.show()

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "root")

    personDF.write
      .mode(SaveMode.Append)
      .jdbc("jdbc:mysql://localhost:3306/test", "inserttest", connectionProperties)


    spark.stop()
  }
}
