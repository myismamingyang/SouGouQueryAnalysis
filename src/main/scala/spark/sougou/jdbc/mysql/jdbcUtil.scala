package spark.sougou.jdbc.mysql

import org.apache.spark.rdd.RDD
import spark.sougou.encapsulation.SogouRecord

import java.sql.{Connection, DriverManager, ResultSet, Statement}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/9 16:59
 * @Version: 1.0
 * @Function:
 */
object jdbcUtil {
  def main(args: Array[String]): Unit = {

    var ip: String = "node3"
    var database: String = "SouGouQueryAnalysis"
    var user: String = "root"
    var cipher: String = "MMYqq123"
    var tablename: String = "scala_JDBC_test"

    var column0 = "id"
    var column1 = "commit_log"
    var column2 = "commit_time"
    var columns: Array[String] = Array[String](column1, column2)

    var data: String = "'sql-3','2023-06-11 18:20:03'"

    // insert into scala_JDBC_test(commit_log,commit_time) values("sql-1",date_format(now(),'%Y-%c-%d %h:%i:%s' ));
    // insert into scala_JDBC_test(commit_log,commit_time) values("sql-1","2023-06-10 23:56:30");
    insert(ip, database, user, cipher, tablename, columns, data)
    selectSingle(ip, database, user, cipher, tablename, columns)
  }

  def insert(ip: String, database: String, user: String, cipher: String, tablename: String, columns: Array[String], data: String): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    //val url = "jdbc:mysql://node3:3306/SouGouQueryAnalysis"
    var url = "jdbc:mysql://" + ip + ":3306/" + database
    var username = user
    var password = cipher

    var connection: Connection = null
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val ps: Statement = connection.createStatement()
    //val sql = s"insert into stu (id, score) values($id , $score)"
    println("sqlstr: " + "insert into " + tablename + "(" + columns(1) + "},${" + columns(2) + "}) values(${" + data + "})")
    var sqlstr: String = s"insert into ${tablename} (${columns(1)},${columns(2)}) values(${data})"

    ps.executeUpdate(sqlstr)
    println("数据插入完成")
    ps.close()
    connection.close()
  }

  def selectSingle(ip: String, database: String, user: String, cipher: String, tablename: String, columns: Array[String]): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    //val url = "jdbc:mysql://node3:3306/SouGouQueryAnalysis"
    var url = "jdbc:mysql://" + ip + ":3306/" + database
    var username = user
    var password = cipher
    var connection: Connection = null
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val ps: Statement = connection.createStatement()
    var sqlstr: String = s"select ${columns(0)},${columns(1)},${columns(2)} from ${tablename} ;"
    println("sqlstr: " + sqlstr)
    val set: ResultSet = ps.executeQuery(sqlstr)
    while (set.next()) {
      println(
        set.getString(columns(0))
        , set.getString(columns(1))
        , set.getString(columns(2))
      )
    }
    connection.close()
  }
}
