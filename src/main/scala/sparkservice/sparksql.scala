package sparkservice


import java.sql.{Connection, DriverManager}
import java.util
import java.util.Properties

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: zjf 
  * @Date: 2019/6/23 23:15 
  * @Description:
  */
class sparksql {


  val conf = new SparkConf().setAppName("TestMysql").setMaster("local")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val url = "jdbc:mysql://127.0.0.1:3306/spark"
  val properties = new Properties()
  properties.setProperty("user", "root")
  properties.setProperty("password", "123456")

  def main(): Unit = {
    println(conf);
  }


  def getUserInfo(): util.List[Row] = {

    //需要传入Mysql的URL、表明、properties（连接数据库的用户名密码）
    val userdf = sqlContext.read.jdbc(url, "user", properties)
    var list = userdf.collectAsList();

    return userdf.collectAsList();

  }


  def getRatingInfo(ids: String): util.List[Row] = {

    //需要传入Mysql的URL、表明、properties（连接数据库的用户名密码）
    val ratingsdf = sqlContext.read.jdbc(url, "ratings", properties)

    var list = ratingsdf.where("userId in (" + ids + ")").collectAsList();

    return list;

  }

  def getRatingInfoBySort(ids: String):util.List[Row]={
    val ratingsdf = sqlContext.read.jdbc(url, "ratings", properties)

    ratingsdf.createOrReplaceTempView("ratings")
    var list = sqlContext.sql("select * from ratings where userId in ("+ids+") order by movieId").collectAsList()

    return list;
  }

  def getMovieInfo(moviesId: String): util.List[Row] = {
    val df = sqlContext.read.jdbc(url, "movies", properties)

    df.createOrReplaceTempView("movies")
    var list = sqlContext.sql("select * from movies where " + "movieId in (" + moviesId + ") and left(movies.genres, 1)<>' ' and left(movies.genres, 3)<>'000' and genres !='(no genres listed)' and genres !='The (1972)\"'").collectAsList()

    return list;
  }


  def getUserAge(): util.List[Row] = {
    val df = sqlContext.read.jdbc(url, "user", properties)

    df.createOrReplaceTempView("user")
    var list = sqlContext.sql("select DISTINCT(Age) from user order by Age").collectAsList()

    return list;
  }


  def getUserWork: util.List[Row] = {
    val df = sqlContext.read.jdbc(url, "user", properties)

    df.createOrReplaceTempView("user")
    var list = sqlContext.sql("select DISTINCT(Occupation) from user order by Occupation").collectAsList()

    return list;
  }

  def showdata(): Unit = {

    val username = "root"
    val password = "123456"
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/spark"
    var connection: Connection = null
    try {

      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select UserID from user")
      while (resultSet.next()) {
        println(resultSet.getString("UserID"))
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      connection.close()
    }

  }


  def sayHello(x: String): Unit = {
    var y: Float = 0
    println("hello, " + x)
  }

}
