package spark.mysqlJDBC

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/7/5 14:52
 * @Version: 1.0
 * @Function: spark 读取txt数据文件 生成schema 后将数据insert到 mysql
 */

object sparkReadWordJDBC {
  case class windowsMYSQL(
                           id: Int
                           , create_date_time: String
                           , session_id: String
                         )

  case class linuxMYSQL(
                         id: Int
                         , commit_log: String
                         , commit_time: String
                       )

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("data/datafile.txt")

    //windowsMYSQL
    val windowsSQLRDD: RDD[windowsMYSQL] = lines.map(line => {
      val arr: Array[String] = line.split(" ")
      windowsMYSQL(
        arr(0).toInt
        , arr(1)
        , arr(2))
    })
    import spark.implicits._
    val windowsMYSQLRDDDF: DataFrame = windowsSQLRDD.toDF()

    windowsMYSQLRDDDF.printSchema()

    val windowsMYSQLup = new java.util.Properties()
    windowsMYSQLup.put("user", "root")
    windowsMYSQLup.put("password", "root")

    windowsMYSQLRDDDF.write
      .mode(SaveMode.Append)
      .jdbc("jdbc:mysql://localhost:3306/test", "inserttest", windowsMYSQLup) //windows MYSQL

    println("已完成sql插入 windowsMYSQL")
    windowsMYSQLRDDDF.show()

    //    // linuxMYSQL
    //    val linuxSQLRDD: RDD[linuxMYSQL] = lines.map(line => {
    //      val arr: Array[String] = line.split(" ")
    //      linuxMYSQL(
    //        arr(0).toInt
    //        , arr(1)
    //        , arr(2))
    //    })
    //    import spark.implicits._
    //    val linuxMYSQLRDDDF: DataFrame = linuxSQLRDD.toDF()
    //
    //    linuxMYSQLRDDDF.printSchema()
    //
    //
    //    val linuxMYSQLup = new java.util.Properties()
    //    linuxMYSQLup.put("user", "root")
    //    linuxMYSQLup.put("password", "MMYqq123")
    //
    //    linuxMYSQLRDDDF.write
    //      .mode(SaveMode.Append)
    //      .jdbc("jdbc:mysql://node3:3306/SouGouQueryAnalysis", "scala_JDBC_test", linuxMYSQLup) //windows MYSQL
    //    println("已完成sql插入 linuxMYSQL")
    //    linuxMYSQLRDDDF.show()


    spark.stop()
  }
}
