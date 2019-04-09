package daily.scalatry

/**
  * Created by adimn on 2019/4/9.
  */
import scala.io.Source

class FileConn {

}

object FileConn{
  def main(args: Array[String]) {
    val source = Source.fromFile("C://Users/adimn/Desktop/back/names_base.txt")
    reverLines(source)

    val urls = Source.fromURL("http://www.baidu.com","UTF-8")
    println(urls.mkString)
  }

  def reverLines(source: Source): Unit ={
    val lines = source.getLines
    //    for(per <- lines) println(per)

    val alines  = source.getLines.toArray
//        for(per <- alines) println(per)

    //    val str =
    println( source.mkString)
    source.close()
  }

  def reverChars(source:Source): Unit ={

  }

}

