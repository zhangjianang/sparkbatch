package daily


import scala.collection.mutable.ArrayBuffer

/**
  * Created by adimn on 2019/4/9.
  */
package object scalatry {
  def main(args: Array[String]) {
//    righitAdd
//    mapRight
    quoteRun
  }
  def wrongAdd(): Unit ={
    val c1 = new Class;
    val ang = c1.register("ang")
    println(ang)
    c1.student += ang
    println(c1)
    val c2  = new Class
    //想要扩大内部类作用域  1、伴生对象，2、投影
//        c2.student += ang
    println(c2)
  }

  def righitAdd(): Unit ={
    val c1 =new ClassObj
    val jack = c1.register("jack")
    c1.student+=jack

    val c2 =new ClassObj
    val lili = c2.register("lili")
    c1.student += lili
    println(c1.student.toString())

  }
  def mapRight(): Unit ={
    val c1 =new ClassMapping
    val harry = c1.register("harry")
    c1.student += harry
    val c2 =new ClassMapping
    val potter = c2.register("potter")
    c1.student  += potter
    println(c1.student.toString())
  }

  def quoteRun(): Unit ={
    val cq = new ClassQuote("c1")
    val stu = cq.registerQuote("ang")
    stu.introduceMyself()
  }

}
 class Class{
   class Student(val name:String)
   val student = new ArrayBuffer[Student]()
   def register(name:String)={
     new Student(name)
   }
 }
//伴生对象，scala中没有静态字段或者方法
object ClassObj{
  class Student(val name:String)
}
class ClassObj{
  val student = new ArrayBuffer[ClassObj.Student]()
  def register(name:String)={
    new ClassObj.Student(name)
  }
}

class ClassMapping{
  class Student(val name:String)
  val student = new ArrayBuffer[ClassMapping#Student]()
  def register(name:String):Student = {
    new Student(name)
  }
}

class ClassQuote(val cname:String){
  outer => class Student(val name:String){
    def introduceMyself()={
      println("my name is "+name+", i am happy to join "+outer.cname)
    }
  }

  def registerQuote(name:String)={
    new Student(name)
  }
}