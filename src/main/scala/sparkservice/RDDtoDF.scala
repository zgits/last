package sparkservice

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @Author: zjf 
  * @Date: 2019/7/3 19:17 
  * @Description:
  */
object RDDtoDF {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder().appName("RDDtoDF").master("local[2]").getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext


    import sparkSession.implicits._
    val employeeDF =
      sc.textFile("employee.txt").map(_.split(",")).map(attributes =>
        Employee(attributes(0).trim.toInt,attributes(1), attributes(2).trim.toInt)).toDF()
    employeeDF.createOrReplaceTempView("employee")
    val employeeRDD = sparkSession.sql("select id,name,age from employee")
    employeeRDD.map(t => "id:"+t(0)+","+"name:"+t(1)+","+"age:"+t(2)).show()
  }
}

case class Employee(id:Long,name:String,age:Long)

