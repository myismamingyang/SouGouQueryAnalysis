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
object insert {
  def main(args: Array[String]): Unit = {

    var ip: String = "node3"
    var database: String = "SouGouQueryAnalysis"
    var user: String = "root"
    var cipher: String = "MMYqq123"
    var tablename: String = "scala_JDBC_test"

    var column1 = "commit_log"
    var column2 = "commit_time"
    var columns: Array[String] = Array[String](column1)
    var data: String = "'sql-1','2023-06-10 23:56:30'"

    // insert into scala_JDBC_test(commit_log,commit_time) values("sql-1",date_format(now(),'%Y-%c-%d %h:%i:%s' ));
    // insert into scala_JDBC_test(commit_log,commit_time) values("sql-1","2023-06-10 23:56:30");
    //insert(ip, database, user, cipher, tablename: String, columns, data)
    selectSingle(ip, database, user, cipher, tablename, columns)
  }

  def insert(ip: String, database: String, user: String, cipher: String, tablename: String, columns: String, data: String): Unit = {
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
    var idint = 7
    var commit_logvarchar = "log-7"
    var sqlstr: String = s"insert into $tablename ($columns) values($data)"
    println("sqlstr: " + sqlstr)
    ps.executeUpdate(sqlstr)
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
    var sqlstr: String = s"select ${columns(0)},${columns(1)} from $tablename"
    println("sqlstr: " + sqlstr)
    val set: ResultSet = ps.executeQuery(sqlstr)
    // TODO: 打印 sql 结构字段自动增加
    var setGetString: String = ""
    if (columns.length > 1) {
      println("多字段查询")
      for (i <- 1 to columns.length) {
        //var setGetStringAll: String = "s\"${columns(0)}: \" + set.getString(columns(0))"
        setGetString += ", " + "\"${columns(" + i + ")}: \" + set.getString(columns(" + i + "))"
      }
    } else {
      println("单字段查询")
      setGetString += "\"${columns(" + 0 + ")}: \" + set.getString(columns(" + 0 + "))"
    }
    println("all: " + setGetString)
    while (set.next()) {
      //println(s"${columns(0)}: " + set.getString(columns(0)), s"${columns(1)}: " + set.getString(columns(1)))
      println(s"$setGetString")
    }
    connection.close()
  }
}
