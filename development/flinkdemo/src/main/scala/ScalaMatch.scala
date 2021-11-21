object ScalaMatch {
  def main(args: Array[String]): Unit = {
    var a : Int = 10
    var b : Int = 20
    var operator : Char = '-'
    var result = operator match {
      case '+' => a +b
      case  '-' => a -b
      case '*' => a * b
      case '/' => a /b
      case _ => "illegal"
    }
    println(result)

    //匹配常量
    def describe(x:Any) = x match {
      case 5 => "Int five"
      case "hello" => "String hello"
      case true => "Boolean true"
      case '+' => "Char +"
    }

    //匹配类型
    def describe1(x:Any) = x match {
      case i:Int => "Int"
      case s: String => "String hello"
      case c:Array[Int] => "Array[INt]"
      case someThing =>"something else"
    }

    //匹配数组
    for(arr <- Array(Array(0),Array(1,0),Array(0,1,0))){
      //对一个数组集合进行遍历
      val result = arr match {
        case Array(0) => "0"
        case Array(x,y) => x + "," + y //匹配有两个元素的数组
        case Array(0,_*) => "以0开头的数组"
        case _ => "something else"
      }
      println("result = " + result)
    }
    //匹配列表
    for(list <- Array(List(0),List(1,0),List(0,0,0),List(1,0,0,1))){
      val result = list match {
        case List(0) => "0"
        case List(x,y) => x + "," + y
        case List(0,_*) => "0..."
        case _ => "something else"
      }
      println(result)
    }

    for(list <- Array(List(0),List(1,0))){
      val result = list match {
        case List(0) => "0"
        case _ => "something else"
      }
    }

    //匹配列表
    val list :List[Int] = List(1,2,4,5,7)

    list match {
      case first :: second :: rest => println(first + "-" + second + "- " + rest)
      case _  => println("something esle")
    }

    val List(first,second,third)= List(1,2,3)
    println(s"$first + $second + $third")

    //匹配元组
    for(tuple <- Array((0,1),(1,0))){
      val result = tuple match {
        case (0,_) => "0，，，，"
        case (y,0) => "...0"
        case (a,b) => "a b"
        case _ => "something else"
      }
    }


  }

}
