package spark.sougou.jdbc.mysql

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/7/7 10:12
 * @Version: 1.0
 * @Function: DataFrame JDBC 插入数据
 */
object insertSouGouQueryAnalysis {
  def insertSchema(tablename: String, searchDataDF: DataFrame): Unit = {

    val linuxMYSQLup = new java.util.Properties()
    linuxMYSQLup.put("user", "root")
    linuxMYSQLup.put("password", "MMYqq123")

    val windowsMYSQLup = new java.util.Properties()
    windowsMYSQLup.put("user", "root")
    windowsMYSQLup.put("password", "root")
    searchDataDF.printSchema()

    searchDataDF.write
      .mode(SaveMode.Append)
      //.jdbc("jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8", tablename, windowsMYSQLup) //windows MYSQL
      .jdbc("jdbc:mysql://node3:3306/SouGouQueryAnalysis?characterEncoding=UTF-8", tablename, linuxMYSQLup) //linux MYSQL

    println(" searchDataDF 已完成sql插入 " + tablename)
    searchDataDF.show()
  }
}
