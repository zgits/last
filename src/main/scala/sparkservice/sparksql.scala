package sparkservice


import java.sql.{Connection, DriverManager}
/**
  * @Author: zjf 
  * @Date: 2019/6/23 23:15 
  * @Description:
  */
class sparksql {

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
          case e: Exception=> e.printStackTrace()
        } finally {
          connection.close()
        }

  }


    def sayHello(x: String): Unit = {
      println("hello, " + x)
    }

  }
