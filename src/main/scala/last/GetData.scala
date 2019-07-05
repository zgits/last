package last

import java.sql.{Connection, DriverManager}
import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.io.PrintWriter
import java.io.File
import java.text.SimpleDateFormat
import java.util.Date



class rating{
  var userId : Integer = 0
  var movieId : Integer = 0
  var ratings : Double = 0.0
  var time : Long = 0

  def setUserId(userid : Int){userId = userid}

  def setMovieId(movieid : Int) {movieId = movieid }

  def setRating(rat : Double) {ratings = rat }

  def setDate(date : Long){ time = date}

  def getUserId() : Int ={
    userId
  }
  def getMovieId() : Int ={ movieId}
  def getRating() : Double ={ ratings}
  def getTime() : Long ={time}
}


class GetData {
  var connection: Connection = _
  val list1=ArrayBuffer[rating]()

  def test(movieId: Int): util.List[rating]={
    // 访问本地MySQL服务器，通过3306端口访问mysql数据库
    val url = "jdbc:mysql://127.0.0.1:3306/spark"
    //驱动名称
    val driver = "com.mysql.jdbc.Driver"
    //用户名
    val username = "root"
    //密码
    val password = "123456"

    try {
      //注册Driver
      Class.forName(driver)
      //得到连接
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement
      //执行查询语句，并返回结果
      val result= statement.executeQuery("select * from ratings where movieId="+movieId+" order by timestamp")
      //打印返回结果
      while (result.next) {
        var rs=new rating()
        rs.setUserId(result.getString("userId").toInt)
        rs.setMovieId( result.getString("movieId").toInt)
        rs.setRating(result.getString("rating").toDouble)
        rs.setDate(result.getString("timestamp").toLong)

        list1.append(rs)
      }
    } catch {
      case e: Exception => e.printStackTrace
    }
    //关闭连接，释放资源
    //connection.close
    //val list: java.util.List[rating] = list1

    //list1.map{x:rating => println(x.movieId, x.time._1, x.time._2)}
    println("++++++++++++++++++++++++++++++++++")
    val list=list1.sortWith{
      case (rat1,rat2)=>{
        rat1.time==rat2.time match {
          case true=> rat1.time>rat2.time //年龄一样，按名字降序排
          case false=>rat1.time<rat2.time //否则按年龄升序排
        }
      }
    }
    list.map{x:rating => println(x.movieId, x.time)}
    return list
  }

  def tranTimeToString(tm:String) :(Int, Int)={
    //val fm = new SimpleDateFormat("dd HH")
    //val fm = new SimpleDateFormat("yyyy")

    val fm = new SimpleDateFormat("yyyy/MM/dd:HH:mm:ss")
    val tim = fm.format(new Date(tm.toLong))
    println(tim)
    println((new Date(tm.toLong).getMonth, new Date(tm.toLong).getHours))
//    (1, 1)
    (new Date(tm.toLong).getDay(), new Date(tm.toLong).getHours())
  }
}

class WriteFile{
  def RecordRating(rat : rating): Unit =
  {
    val writer = new PrintWriter(new File("D:\\sparktest\\12.csv"))
    writer.println(rat.getUserId() + "," + rat.getMovieId() + "," + rat.getRating() + "," + rat.getTime())
    writer.close()
  }
}
