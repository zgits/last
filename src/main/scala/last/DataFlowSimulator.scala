package last

import java.io.PrintWriter
import java.net.ServerSocket
import scala.io.Source
import java.net.{InetAddress,ServerSocket,Socket,SocketException}

object DataFlowSimulator {
  //定义随机获取整数的方法
  def index(length:Int)={
    import java.util.Random
    val rdm = new Random();
    rdm.nextInt(length)
  }

  def main(args: Array[String]): Unit = {

    //调用该模拟器需要三个参数，分为文件路径、端口好、间隔时间(单位：毫秒)
//    if(args.length != 3){
    ////      System.err.println("Usage <filename> <port> <millisecond>")
    ////      System.exit(-1)
    ////    }

    //获取文件的总行数
    val filename = "D:\\sparktest\\test.csv"
    val lines = Source.fromFile(filename).getLines().toList
    val filerow = lines.length

    //制定端口，但外部程序请求时建立连接
    val lister = new ServerSocket("9998".toInt)
    while (true){
      println("Test start")
      //val socket = lister.accept()

      val socket = new Socket("127.0.0.1", 9998)
      println(socket.isConnected())
      new Thread(){
        override def run(): Unit ={
          println("Got client connection from:"+socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream,true)

//          while(true){
//            Thread.sleep(5)
//            //当该端口接受请求时，随机获取某以行的数据
//            val content = lines(index(filerow))
//            println(content)
//            out.write(content+"\n")
//            out.flush()
//          }
          Thread.sleep(5)
          //当该端口接受请求时，随机获取某以行的数据
          val content = lines(index(filerow))
          println(content)
          out.write(content+"\n")
          out.flush()
          out.close()
          socket.close()
        }
      }.start()
    }

  }
}
