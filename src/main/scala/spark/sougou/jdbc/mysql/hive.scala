package spark.sougou.jdbc.mysql

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/10 20:51
 * @Version: 1.0
 * @Function:
 */
object hive {
  def main(args: Array[String]): Unit = {
    // 1.加载驱动
    Class.forName("org.apache.hive.jdbc.HiveDriver") //看上去像是在使用Hive的server2,本质上使用Spark的ThriftServer
    // 2.获取连接Connection
    val conn: Connection = DriverManager.getConnection(
      "jdbc:hive2://node2:10000/default", //看上去像是在使用Hive的server2,本质上使用Spark的ThriftServer
      "root",
      "123456"
    )
    // 3.构建查询语句
    val sqlStr: String = """select * from person"""
    val ps: PreparedStatement = conn.prepareStatement(sqlStr)
    // 4.执行查询，获取结果
    val rs: ResultSet = ps.executeQuery()
    // 5.处理查询结果
    while (rs.next()) {
      println(s"id = ${rs.getInt(1)}, name = ${rs.getString(2)}, age = ${rs.getInt(3)}}")
    }
    if (null != rs) rs.close()
    if (null != ps) ps.close()
    if (null != conn) conn.close()
  }
}
