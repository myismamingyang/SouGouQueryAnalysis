package spark.sougou.jdbc.mysql

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/7/7 10:12
 * @Version: 1.0
 * @Function:
 */
object insertSouGouQueryAnalysis {
  def insertSchema(tablename: String, searchDataDF: DataFrame): Unit = {

    val linuxMYSQLup = new java.util.Properties()
    linuxMYSQLup.put("user", "root")
    linuxMYSQLup.put("password", "MMYqq123")

    searchDataDF.printSchema()

    searchDataDF.write
      .mode(SaveMode.Append)
      .jdbc("jdbc:mysql://node3:3306/SouGouQueryAnalysis", tablename, linuxMYSQLup) //windows MYSQL

    println(s" searchDataDF 已完成sql插入 ${tablename}")
    searchDataDF.show()
  }
}
