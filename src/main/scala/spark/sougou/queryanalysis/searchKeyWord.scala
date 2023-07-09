package spark.sougou.queryanalysis

import com.hankcs.hanlp.HanLP
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.sougou.encapsulation.SogouRecord

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.StringOps
import scala.collection.mutable

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/8 23:46
 * @Version: 1.0
 * @Function: 3.1搜索关键词统计(word,数量),注意:词要做切割
 */
object searchKeyWord {
  def statistics(sc: SparkContext, recordRDD: RDD[SogouRecord.Sogou]): RDD[SogouRecord.searchKeyWord] = {
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

    val now: LocalDateTime = LocalDateTime.now()
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val formattedDateTime: String = now.format(formatter)

    val result1Schema: Array[SogouRecord.searchKeyWord] =
      result1.map { case (searchWord, wordCount) =>
        SogouRecord.searchKeyWord(searchWord, wordCount, commitTime = formattedDateTime)
      }
    val result1RDD: RDD[SogouRecord.searchKeyWord] = sc.parallelize(result1Schema)

    result1RDD
  }
}
