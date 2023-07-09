package spark.sougou.driver

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import spark.sougou.encapsulation.SogouRecord
import spark.sougou.queryanalysis.{searchKeyWord, searchTimePeriod, userSearchVocabulary}
import spark.sougou.jdbc.mysql.insertSouGouQueryAnalysis

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/8 0:08
 * @Version: 1.0
 * @Function: 搜狗搜索日志分析
 */
object SouGouDrivers {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val logRDD: RDD[String] = sc.textFile("data/input/SogouQ.sample")

    val recordRDD: RDD[SogouRecord.Sogou] = logRDD.filter(StringUtils.isNotBlank(_)) //过滤出合法数据
      .map(line => {
        val arr: Array[String] = line.split("\\s+") //每行切分入厂
        SogouRecord.Sogou(
          arr(0),
          arr(1),
          arr(2),
          arr(3).toInt,
          arr(4).toInt,
          arr(5)
        )
      })

    println("搜索关键词统计Top10统计")
    searchKeyWord.statistics(sc, recordRDD).foreach(println)
    println("用户搜索词汇统计Top10统计")
    userSearchVocabulary.statistics(sc,recordRDD).foreach(println)
    println("搜索时间段统计")
    searchTimePeriod.statistics(sc,recordRDD).foreach(println)

    val searchWordRDD: RDD[SogouRecord.searchKeyWord] = searchKeyWord.statistics(sc, recordRDD)
    val searchWordDataFrame: DataFrame = spark.createDataFrame(searchWordRDD)
    //import spark.implicits._
    val searchWordDF: DataFrame = searchWordDataFrame.toDF()
    insertSouGouQueryAnalysis.insertSchema("searchKeyWord",searchWordDF)

    val userSearchVocabularyRDD: RDD[SogouRecord.userSearchVocabulary] = userSearchVocabulary.statistics(sc, recordRDD)
    val userSearchVocabularyDataFrame: DataFrame = spark.createDataFrame(userSearchVocabularyRDD)
    //import spark.implicits._
    val userSearchVocabularyDF: DataFrame = userSearchVocabularyDataFrame.toDF()
    insertSouGouQueryAnalysis.insertSchema("userSearchVocabulary",userSearchVocabularyDF)

    val searchTimePeriodRDD: RDD[SogouRecord.searchTimePeriod] = searchTimePeriod.statistics(sc, recordRDD)
    val searchTimePeriodDataFrame: DataFrame = spark.createDataFrame(searchTimePeriodRDD)
    //import spark.implicits._
    val searchTimePeriodDF: DataFrame = searchTimePeriodDataFrame.toDF()
    insertSouGouQueryAnalysis.insertSchema("searchTimePeriod",searchTimePeriodDF)
  }
}
