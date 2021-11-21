package com.demo

/**
 * @author kevin
 * @date 2021/10/29
 * @desc
 */
object Binarysearch {

  def main(args: Array[String]): Unit = {

  }

  def  binarySearch(arr:Array[Int],left:Int,right:Int,finadVal:Int):Int={
    if(left > right){
      //递归出条件
      -1
    }

    val midIndex = (left + right)/2

    if(finadVal < arr(midIndex)){
      //向左递归查找
      binarySearch(arr, left, midIndex -1, finadVal)
    }else if(finadVal > arr(midIndex)){
      //向右递归查找
      binarySearch(arr,midIndex + 1, right, finadVal)
    }else{
      //返回下标
      midIndex
    }
  }

}
