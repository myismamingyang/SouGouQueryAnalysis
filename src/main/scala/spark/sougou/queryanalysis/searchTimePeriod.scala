package spark.sougou.queryanalysis

import org.apache.spark.rdd.RDD
import spark.sougou.encapsulation.SogouRecord

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/8 23:46
 * @Version: 1.0
 * @Function: 3.3搜索时间段统计(小时:分钟,数量)
 */
object searchTimePeriod {
  case class statistics(recordRDD: RDD[SogouRecord.Sogou]){
        val result3: Array[(String, Int)] = recordRDD.map(record => {
          //00:00:00
          val tiem: String = record.queryTime.substring(0, 5) //[)
          (tiem, 1)
        }).reduceByKey(_ + _)
          .sortBy(_._2, false)
          .take(10)
        println("搜索时间段统计")
        result3.foreach(println)
  }
}
