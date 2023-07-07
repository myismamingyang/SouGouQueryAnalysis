package spark.sougou.encapsulation

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/8 13:48
 * @Version: 1.0
 * @Function1: SogouRecord
 * @param t1 -------------------------------------------------
 * @param tableSchema [ Sogou ]
 * @param queryTime  访问时间，格式为：HH:mm:ss
 * @param userId     用户ID
 * @param queryWords 查询词
 * @param resultRank 该URL在返回结果中的排名
 * @param clickRank  用户点击的顺序号
 * @param clickUrl   用户点击的URL
 * @param t2 -------------------------------------------------
 * @param tableSchema [ searchKeyWord ]
 * @param searchWord  搜索词汇
 * @param wordCount 搜索次数
 * @param commitTime  提交时间
 * @param t3 -------------------------------------------------
 * @param tableSchema [ userSearchVocabulary ]
 * @param userId  用户ID
 * @param searchWord  搜索词汇
 * @param searchCount 搜索次数
 * @param commitTime  提交时间
 * @param t4 -------------------------------------------------
 * @param tableSchema [ searchTimePeriod ]
 * @param searchTime  搜索时间
 * @param searchCount 搜索次数
 * @param commitTime  提交时间
 */
object SogouRecord {
  case class Sogou(
                    queryTime: String,
                    userId: String,
                    queryWords: String,
                    resultRank: Int,
                    clickRank: Int,
                    clickUrl: String
                  )
  case class searchKeyWord(
                    searchWord: String,
                    wordCount: Int,
                    commitTime: String
                  )
  case class userSearchVocabulary(
                            userId: String,
                            searchWord: String,
                            searchCount: Int,
                            commitTime: String
                          )
  case class searchTimePeriod(
                            searchTime: String,
                            searchCount: Int,
                            commitTime: String
                          )
}
