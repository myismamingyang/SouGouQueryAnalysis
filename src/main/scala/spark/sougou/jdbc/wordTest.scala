package spark.sougou.jdbc


import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import spark.sougou.encapsulation.testLibrary.scala_JDBC_test
/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/13 10:01
 * @Version: 1.0
 * @Function:
 */
object wordTest {
  def main(args: Array[String]): Unit = {
//    val driver = "com.mysql.jdbc.Driver"
//    Class.forName(driver)
    //获取sparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("spark_Mysql").master("local[2]").getOrCreate()

    //通过sparkSession得到sparkContext
    val sparkContext: SparkContext = sparkSession.sparkContext

    //通过sparkContext 读取文本文件内容，得到RDD
    val arrRDD: RDD[Array[String]] =
      sparkContext.textFile("data/input/person.txt").map(x => x.split(" "))

    for ( e <- arrRDD){
      println(e(0).mkString + "     0")
    }

    //通过RDD，配合样例类，将我们的数据转换成样例类对象
//    val personRDD: RDD[scala_JDBC_test] = arrRDD.map(
//      x => scala_JDBC_test(
//        x(0).toInt
//        ,x(1)
//        ,x(2)
//      )
//    )
//
//    //导入sparkSession当中的隐式转换，将我们的样例类对象转换成DataFrame
//    import sparkSession.implicits._
//    val personDF: DataFrame = personRDD.toDF()
//
//    //打印dataFrame当中的数据
//    val personDFShow: Unit = personDF.show()
//
//    //将DataFrame注册成为一张表模型
//    val personView: Unit = personDF.createTempView("person_view")
//
//    //获取表当中的数据
//    val result: DataFrame = sparkSession.sql("select * from person_view")
//
//    result.show()
//    //获取mysql连接
//    val url ="jdbc:mysql://node3:3306/testLibrary?useSSL=false"
//    val tableName = "scala_JDBC_test"
//    val properties = new Properties()
//    properties.setProperty("user","root")
//    properties.setProperty("password","MMYqq123")
//
//    //将我们查询的结果写入到mysql当中去
//    val unit: Unit = result.write.mode(SaveMode.Overwrite).jdbc(url, tableName, properties)
//    println(unit)
//    sparkContext.stop()
//    sparkSession.close()
  }
}
