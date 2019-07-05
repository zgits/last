package last

import java.sql.{ Connection, DriverManager }
object GetRatingData {
  var connection: Connection =_
  def main(args: Array[String]): Unit = {
    val url = "jdbc:mysql://localhost:3306/cgjr?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    //驱动名称
    val driver = "com.mysql.jdbc.Driver"
    //用户名
    val username = "root"
    //密码
    val password = "12345"
    //初始化数据连接

    try {
      //注册Driver
      Class.forName(driver)
      //得到连接
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement
      //执行查询语句，并返回结果
      val rs  = statement.executeQuery("SELECT * FROM persons WHERE movieID = ")
      //打印返回结果
      while (rs.next) {
        val userId = rs.getString("userID")
        val movieId = rs.getString("movieID")
        val rating = rs.getString("rating")
        val timeStamp = rs.getString("timeStamp")
        //      println(name+"\t"+num)
        println("name = %s, num = %s".format(rating, timeStamp))
      }
    }
    catch {
      case e: Exception => e.printStackTrace
    }
  }
}
