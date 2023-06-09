package spark.sougou.jdbc.mysql

import org.apache.spark.sql.SparkSession

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/9 9:31
 * @Version: 1.0
 * @Function:
 */
object createTable {
  def newTables(table:String): Unit = {
    val spark = SparkSession.builder().appName("MysqlQueryDemo").master("local").getOrCreate()
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://node3:3306/testLibrary?useUnicode=true&characterEncoding=utf-8")
      .option("driver", "com.mysql.jdbc.Driver") //驱动
      .option("user", "root")
      .option("password", "MMYqq123")
      //.option("dbtable", s"(select COUNT(1) from customer) customer") 直接查询sql数据
      .option("dbtable", table) //加载表  // dbtable: 指定查询功能
      // s"(sql) dbtable"  为查询sql语法
      .load()
    jdbcDF.show()
  }
}
