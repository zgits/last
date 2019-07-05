package last


import java.text.SimpleDateFormat
import java.util.Date
import java.io.PrintWriter
import java.io.File
//
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark._
//import org.apache.spark.streaming._
//import org.apache.spark.storage.StorageLevel

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object sparktest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("streaming").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(20))
    val lines = ssc.textFileStream("C:\\Users\\幻夜~星辰\\Desktop")
    //    val words = lines.flatMap(_.split("|"))
    val wordCounts = lines.map(x => (x,1)).reduceByKey(_+_)
    wordCounts.print()
    wordCounts.foreachRDD(rdd => {
      def func(recoeds: Iterator[(String,Int)]): Unit ={
        var conn:Connection = null
        var stmt:PreparedStatement = null
        try{
          val url = "jdbc:mysql://127.0.0.1:3306/spark"
          val user = "root"
          val password = "123456"
          conn = DriverManager.getConnection(url,user,password)
          recoeds.foreach(p => {
            val sql = "insert into ratings(userId, movieId,rating,timestamp) values (?,?,?,?)"
            stmt = conn.prepareStatement(sql)
            val data :Array[String] = p._1.split(",")
//            stmt.setString(1,data(0).trim)
//            stmt.setString(2,data(1).trim)
//            stmt.setString(3,data(2).trim)
//            stmt.setInt(4,p._2.toInt)
            stmt.setInt(1,data(0).toInt)
            stmt.setInt(2,data(1).toInt)
            stmt.setDouble(3,data(2).toDouble)
            stmt.setString(4,tranTimeToLong(data(3).toString).toString)
            //stmt.setString(4,tranTimeToLong(p._2.toString))
            stmt.executeUpdate()
          })
        }
        catch{
          case exception: Exception => exception.printStackTrace()
        }
        finally {
          if(stmt != null){
            stmt.close()
          }
          if(conn != null){
            conn.close()
          }
        }
      }
      val repartitionedRDD = rdd.repartition(3)
      repartitionedRDD.foreachPartition(func)
    })
    ssc.start()
    ssc.awaitTermination()
  }


//  def tranTimeToString(tm:String) :String={
//    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val tim = fm.format(new Date(tm.toLong))
//    tim
//  }

  //时间转换成时间戳
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyy/MM/dd:HH:mm:ss")
    println(tm)
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()
    tim
  }

  def WriteRatingFile(unit: Array[Array[String]]) ={
    val writer = new PrintWriter(new File("D:\\a.csv"))
    writer.println(unit)
    writer.close()
  }
}
