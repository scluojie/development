
//测试泛型不可变
object ScalaGeneric {

  def main(args: Array[String]): Unit = {
   def f[A:Test](a:A) = println(a)
    implicit val test:Test[User] = new Test[User]
    f(new User())
  }

  def  test[A>:User](a:A):Unit ={
    println(a)
  }

  //泛型协变[+T]:就是将子类型当成父类类型来使用
  //泛型协变[-T]:将父类型当成子类型来使用
  class Test[T]{

  }
  class Parent{

  }
  class User extends Parent {

  }
  class SubUser extends User{

  }

  class Emp{

  }
}
