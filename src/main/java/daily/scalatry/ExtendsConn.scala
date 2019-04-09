package daily.scalatry

import scala.collection.mutable.ArrayBuffer

/**
  * Created by adimn on 2019/4/9.
  */
class ExtendsConn {

}
class Student{
  val num:Int = 10
  val scores :Array[Int]= new Array[Int](num)
}

class PEStudent extends Student{
  override val num:Int = 2
}

object ExtendsObj{
  def main(args: Array[String]) {
    val ps = new PEStudent
    //由于先执行父类的构造函数，但是有重载的，会重写getter方法，返回零
    println(ps.scores.length)

    //这时需要让子类的构造函数提前执行， 再执行父类的构造函数时，就能正确创建了
    val rps = new PrePEStudent
    println(rps.scores.length  )

    testEq

  }
  def testEq(): Unit ={
    val p1 =new Person("ang",10.0)
    val p2 =new Person("ang",10.0)
    println(" == " + (p1 == p2))
    println(" equals "+p1.equals(p2))
    val p3 = p1
    println(" == " + ( p3 == p1 ))
    println(" equals "+p3.equals(p1))
  }
}

class PrePEStudent extends {
  override val num:Int = 2
}with  Student

class Person(val name:String,val price:Double){
  final override def equals(other:Any):Boolean={
    if(name == other.asInstanceOf[Person].name && price == other.asInstanceOf[Person].price){
      return true
    }
     return false
  }
}