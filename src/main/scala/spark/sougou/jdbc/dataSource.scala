package spark.sougou.jdbc

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * @Author: Mingyang Ma
 * @Date: 2023/6/15 16:06
 * @Version: 1.0
 * @Function:
 */
object dataSource {
  def main(args: Array[String]): Unit = {
    //1.准备SparkSQL开发环境
    val spark: SparkSession = SparkSession.builder().appName("hello").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val fileRDD: RDD[String] = sc.textFile("data/input/person.txt")
    val personRDD: RDD[Person] = fileRDD.map(line => {
      val arr: Array[String] = line.split(" ")
      Person(arr(0).toInt, arr(1), arr(2))
    })
    import spark.implicits._ //隐式转换
    val df: DataFrame = personRDD.toDF()
    //df.printSchema()
    //df.show(false)

//    //TODO 写
//    //df.coalesce(1).write.mode(SaveMode.Overwrite)
//    //.text("data/output/text")//注意:往普通文件写不支持Schema
//    df.coalesce(1).write.mode(SaveMode.Overwrite).json("data/output/json")
//    df.coalesce(1).write.mode(SaveMode.Overwrite).csv("data/output/csv")
//    df.coalesce(1).write.mode(SaveMode.Overwrite).parquet("data/output/parquet")
    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","MMYqq123")
    //df.coalesce(1).write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://node3:3306/testLibrary?characterEncoding=UTF-8","person",prop)//表会自动创建
    //df.coalesce(1).write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://node3:3306/testLibrary?characterEncoding=UTF-8&useSSL=false","person",prop)//表会自动创建

//    //TODO 读
//    //spark.read.text("data/output/text").show(false)
//    spark.read.json("data/output/json").show(false)
//    spark.read.csv("data/output/csv").toDF("id1","name1","age1").show(false)
//    spark.read.parquet("data/output/parquet").show(false)
    println("spark")
   spark.read.jdbc("jdbc:mysql://node3:3306/testLibrary?characterEncoding=UTF-8","scala_JDBC_test",prop).show(false)
    println("spark")
    sc.stop()
    spark.stop()
  }
  case class Person(id:Int,name:String,time:String)
}
