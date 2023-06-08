package spark.sougou.queryanalysis

import com.hankcs.hanlp.HanLP
import org.apache.spark.rdd.RDD
import spark.sougou.encapsulation.SogouRecord

import scala.collection.immutable.StringOps
import scala.collection.mutable

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/8 23:46
 * @Version: 1.0
 * @Function: 3.1搜索关键词统计(word,数量),注意:词要做切割
 */
object searchKeyword {
  case class statistics(recordRDD: RDD[SogouRecord.Sogou]){
    val result1: Array[(String, Int)] = recordRDD.flatMap(record => { //年轻人住房问题 出来: [年轻人,住房,问题]
      import scala.collection.JavaConverters._
      val words: StringOps = record.queryWords //年轻人住房问题
      val splitedWords: mutable.Buffer[String] = HanLP.segment(words.replaceAll("\\[|\\]", ""))
        .asScala.map(_.word.trim)
      splitedWords //[年轻人,住房,问题]
    })
      .filter(word => !word.equals(".") && !word.equals("+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)
    println("搜索关键词统计Top10统计")
    result1.foreach(println)
  }
}
