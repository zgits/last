package scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.{SparkConf, SparkContext}

object searchMovies {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  val inputFile = "hdfs://192.168.100.20:9000/spark/movies.csv"
  System.setProperty("hadoop.home.dir", "E:\\Program Files\\学习\\spark\\hadoop\\hadoop")
  val conf = new SparkConf().setMaster("local").setAppName("searchMovies").set("spark.driver.allowMultipleContexts","true")
  val sc = new SparkContext(conf)
  def search(movieName:String): JavaRDD[String] ={
    val rdd = sc.textFile(inputFile).filter(line => line.split(",")(1).contains(movieName)).toJavaRDD()
    return rdd
  }
}
