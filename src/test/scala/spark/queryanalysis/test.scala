package spark.queryanalysis

import com.hankcs.hanlp.HanLP
import org.apache.spark.rdd.RDD
import spark.sougou.encapsulation.SogouRecord

import scala.collection.immutable.StringOps
import scala.collection.mutable


/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/8 23:46
 * @Version: 1.0
 * @Function: 3.2用户搜索词汇统计
 */
object test {
  //def statistics(recordRDD: RDD[SogouRecord.Sogou]): Array[((String, String),Int)]={
  def statistics(recordRDD: RDD[SogouRecord.Sogou]): Array[sougou]={
    val result2: Array[((String, String),Int)]= recordRDD.flatMap(record => { //年轻人住房问题 出来: [(用户id,年轻人),(用户id,住房),(用户id,问题)]
      import scala.collection.JavaConverters._
      val userId: String = record.userId
      val words: StringOps = record.queryWords
      val splitedWords: mutable.Buffer[String] = HanLP.segment(words.replaceAll("\\[|\\]", ""))
        .asScala.map(_.word.trim)
      val tuples: mutable.Buffer[(String, String)] = splitedWords.map(word => {
        (userId, word)
      })
      tuples
    })
      .filter(t => !t._2.equals(".") && !t._2.equals("+"))
      .map((_, 1)) //[((用户id,年轻人),1),((用户id,住房),1)....]
      .reduceByKey(_ + _) //[((用户id,年轻人),数量),((用户id,住房),数量)....]
      .sortBy(_._2, false)
      .take(10)
    //result2.foreach(println)
    result2

    result2.map{ case ((userId,queryWords),count) => sougou(userId,queryWords,count)}
  }
  case class sougou(
                    userId: String,
                    queryWords: String,
                    count: Int,
                  )
}
