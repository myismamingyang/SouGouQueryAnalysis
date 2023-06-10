package spark.sougou.jdbc.mysql

import java.sql.{Connection, DriverManager, Statement}

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
    var columns: String = "commit_log,commit_time"
    var data: String = "'sql-1','2023-06-10 23:56:30'"

    // insert into scala_JDBC_test(commit_log,commit_time) values("sql-1",date_format(now(),'%Y-%c-%d %h:%i:%s' ));
    // insert into scala_JDBC_test(commit_log,commit_time) values("sql-1","2023-06-10 23:56:30");
    insert(ip, database, user, cipher, tablename: String, columns, data)
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
    //val query = s"insert into stu (id, score) values($id , $score)"
    //ps.addBatch(query)
    var idint = 7
    var commit_logvarchar = "log-7"

    println("columns: " + columns)
    println("data: " + data)
    var sqlstr: String = s"insert into $tablename ($columns) values($data)"
    println("sqlstr: " + sqlstr)
    ps.executeUpdate(sqlstr)
    ps.close()
    connection.close()
  }
}
