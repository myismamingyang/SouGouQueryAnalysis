package spark.sougou.queryanalysis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.sougou.encapsulation.SogouRecord

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/8 23:46
 * @Version: 1.0
 * @Function: 3搜索时间段统计(小时:分钟,数量)
 */
object searchTimePeriod {
  def statistics(sc: SparkContext, recordRDD: RDD[SogouRecord.Sogou]): RDD[SogouRecord.searchTimePeriod] = {
    val result3: Array[(String, Int)] = recordRDD.map(record => {
      //00:00:00
      val tiem: String = record.queryTime.substring(0, 5) //[)
      (tiem, 1)
    }).reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)

    val now: LocalDateTime = LocalDateTime.now()
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val formattedDateTime: String = now.format(formatter)

    val result3Schema: Array[SogouRecord.searchTimePeriod] =
      result3.map { case (searchTime, searchCount) =>
        SogouRecord.searchTimePeriod(searchTime, searchCount, commitTime = formattedDateTime)
      }
    val result3RDD: RDD[SogouRecord.searchTimePeriod] = sc.parallelize(result3Schema)

    result3RDD
  }
}
