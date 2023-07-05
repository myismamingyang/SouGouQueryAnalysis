package spark.sougou.jdbc.mysql

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}


/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/10 20:07
 * @Version: 1.0
 * @Function:
 */
//jdbc:mysql://192.168.63.163:3306/SouGouQueryAnalysis?useUnicode=true&characterEncoding=utf-8
object jdbc_mysql_OK {
  def main(args: Array[String]): Unit = {
    //    // 1.加载驱动
    //    Class.forName("com.mysql.jdbc.Driver") //看上去像是在使用Hive的server2,本质上使用Spark的ThriftServer
    //    // 2.获取连接Connection
    //    val conn: Connection = DriverManager.getConnection(
    //      "jdbc:mysql://node3:3306/SouGouQueryAnalysis", //看上去像是在使用Hive的server2,本质上使用Spark的ThriftServer
    //      "root",
    //      "MMYqq123"
    //    )
    //
    //    // 3.1构建查询语句
    //    val sqlSelect: String = """select * from inserCreat_test"""
    //    val psSelect: PreparedStatement = conn.prepareStatement(sqlSelect)
    //    // 4.执行查询，获取结果
    //    val rsSelect: ResultSet = psSelect.executeQuery()
    //    // 5.处理查询结果
    //    while (rsSelect.next()) {
    //      println(s"id = ${rsSelect.getInt(1)}, name = ${rsSelect.getString(2)}")
    //    }
    //    if (null != rsSelect) rsSelect.close()
    //    if (null != psSelect) psSelect.close()
    //    if (null != conn) conn.close()


    // connect to the database named "mysql" on the localhost


    //    val driver = "com.mysql.jdbc.Driver"
    //    val url = "jdbc:mysql://node3:3306/SouGouQueryAnalysis"
    //    val username = "root"
    //    val password = "MMYqq123"
    //
    //
    //    var connection: Connection = null
    //
    //    Class.forName(driver)
    //    connection = DriverManager.getConnection(url, username, password)
    //    val ps: Statement = connection.createStatement()
    //    //val query = s"insert into stu (id, score) values($id , $score)"
    //    //ps.addBatch(query)
    //    var idint = 7
    //    var commit_logvarchar = "log-7"
    //    var sqlstr: String = s"insert into inserCreat_test (id, commit_log) values($idint,'$commit_logvarchar')"
    //    ps.executeUpdate(sqlstr)
    //    ps.close()
    //    connection.close()
    var ip: String = "localhost"
    var database: String = "test"
    var user: String = "root"
    var cipher: String = "root"
    var tablename: String = "inserttest"
    var columns: String = "id,create_date_time,session_id"
    var data: String = "1,'sql-1','2023-06-10 23:56:30'"

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
