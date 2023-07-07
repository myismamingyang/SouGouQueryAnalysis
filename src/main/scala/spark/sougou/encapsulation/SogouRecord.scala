package spark.sougou.encapsulation

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/8 13:48
 * @Version: 1.0
 * @Function:
 * @param queryTime  访问时间，格式为：HH:mm:ss
 * @param userId     用户ID
 * @param queryWords 查询词
 * @param resultRank 该URL在返回结果中的排名
 * @param clickRank  用户点击的顺序号
 * @param clickUrl   用户点击的URL
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
                    wordCount: String,
                    commitTime: String
                  )
}
