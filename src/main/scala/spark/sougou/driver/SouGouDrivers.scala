package spark.sougou.driver

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spark.sougou.encapsulation.SogouRecord

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/8 0:08
 * @Version: 1.0
 * @Function: 搜狗搜索日志分析
 */
object SouGouDrivers {
  def main(args: Array[String]): Unit = {
    //1.sc
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2.source
    val logRDD: RDD[String] = sc.textFile("data/input/SogouQ.sample")

    //3.transformation
    //3.0转为样例类封装为一条条的记录对象
    val recordRDD: RDD[SogouRecord.Sogou] = logRDD.filter(StringUtils.isNotBlank(_)) //过滤出合法数据
      .map(line => {
        val arr: Array[String] = line.split("\\s+")  //每行切分入厂
        SogouRecord.Sogou(
          arr(0),
          arr(1),
          arr(2),
          arr(3).toInt,
          arr(4).toInt,
          arr(5)
        )
      })

    spark.sougou.queryanalysis.searchKeyword.statistics(recordRDD)
    spark.sougou.queryanalysis.searchTimePeriod.statistics(recordRDD)
    spark.sougou.queryanalysis.userSearchVocabulary.statistics(recordRDD)

  }
}
