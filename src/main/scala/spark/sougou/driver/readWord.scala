package spark.sougou.driver

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spark.sougou.encapsulation.testLibrary

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/11 14:23
 * @Version: 1.0
 * @Function:
 */
object readWord {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val wordText: RDD[String] = sc.textFile("data/input/readWord.txt")
    val value: RDD[testLibrary.scala_JDBC_test] = wordText.filter(StringUtils.isNotBlank(_)).map(line => {
      val arr: Array[String] = line.split("\\s+")
      arr(0).foreach(print)
      arr(1).foreach(print)
      testLibrary.scala_JDBC_test(
        arr(0).toInt,
        arr(1),
        arr(2)
      )
    })




    var ip: String = "node3"
    var database: String = "SouGouQueryAnalysis"
    var user: String = "root"
    var cipher: String = "MMYqq123"
    var tablename: String = "scala_JDBC_test"
    var columns: String = "commit_log,commit_time"
    var data: String = "'sql-1','2023-06-10 23:56:30'"

    //insert(ip, database, user, cipher, tablename: String, columns, value)
    //println(value)
  }
}
