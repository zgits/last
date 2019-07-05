package last

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer



//class rating{
//   var userId : Integer = 0
//   var movieId : Integer = 0
//   var ratings : Double = 0.0
//   var time : String = "0-0-0"
//
//  def setUserId(userid : Int){userId = userid}
//
//  def setMovieId(movieid : Int) { }
//
//  def setRating(rat : Double) { }
//
//  def setDate(date : String){ }
//}

class test {
  var connection: Connection = _
  val list1=ArrayBuffer[rating]()

  def test(movieId: Int): util.List[rating]={
    // 访问本地MySQL服务器，通过3306端口访问mysql数据库
    val url = "jdbc:mysql://localhost:3306/spark?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    //驱动名称
    val driver = "com.mysql.jdbc.Driver"
    //用户名
    val username = "root"
    //密码
    val password = "ty980514"

    try {
      //注册Driver
      Class.forName(driver)
      //得到连接
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement
      //执行查询语句，并返回结果
      val result= statement.executeQuery("select * from rating where movieId="+movieId+" order by date")
      //打印返回结果
      while (result.next) {
        var rs=new rating()
        rs.setUserId(result.getString("userId").toInt)
        rs.setMovieId( result.getString("movieId").toInt)
        rs.setRating(result.getString("rating").toDouble)
        rs.setDate(result.getString("date").toLong)

       list1.append(rs)
      }
    } catch {
      case e: Exception => e.printStackTrace
    }
    //关闭连接，释放资源
    connection.close
    //val list: java.util.List[rating] = list1
    val list=list1.sortWith{
      case (rat1,rat2)=>{
        rat1.time==rat2.time match {
          case true=> rat1.time<rat2.time//年龄一样，按名字降序排
          case false=>rat1.time>rat2.time //否则按年龄升序排
        }
      }
    }
    return list
  }

  def tranTimeToString(tm:String) :(Int, Int)={
    //val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val fm = new SimpleDateFormat("yyyy")
    val tim = fm.format(new Date(tm.toLong).getYear)
    (new Date(tm.toLong).getDay, new Date(tm.toLong).getHours)
  }
}

object Mydata {

  def main(args: Array[String]): Unit = {
//    val test : WriteFile = new WriteFile
//    println(new GetData().tranTimeToString("1425941529"))
//
//    val list2=ArrayBuffer[rating]()
//    var rs= new rating()
//    rs.setUserId(1)
//    rs.setMovieId(2)
//    rs.setRating(3.3)
//    rs.setDate(1996)
//    list2.append(rs)
//    var rs1= new rating()
//    rs1.setUserId(2)
//    rs1.setMovieId(3)
//    rs1.setRating(3.3)
//    rs1.setDate(1998)
//    list2.append(rs1)
//
//    var rs2= new rating()
//    rs2.setUserId(5)
//    rs2.setMovieId(1)
//    rs2.setRating(3.3)
//    rs2.setDate(1997)
//    list2.append(rs2)
//    //list2.sortWith(_.compareTo(_)<0)
//    //先按年龄排序，如果一样，就按照名称降序排
//    val b=list2.sortWith{
//      case (rat1,rat2)=>{
//        rat1.time==rat2.time match {
//          case true=> rat1.time>rat2.time //年龄一样，按名字降序排
//          case false=>rat1.time<rat2.time //否则按年龄升序排
//        }
//      }
//    }
//    b.map{x:rating => println(x.movieId + " " + x.time)}
    //test.RecordRating(rs)
    //val list = new GetData().test(110)
    //list.map{x:rating => println(x.movieId, x.time._1, x.time._2)}
//    val tm = "2019/4/5:10:10:10"
//    val a = tranTimeToLong(tm)
//    println(a)
    //println(tranTimeToLong("2019-5-5"))
    new GetData().test(110);
  }

  //时间转换成时间戳
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyy/MM/dd:HH:mm:ss")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()
    tim
  }
}

