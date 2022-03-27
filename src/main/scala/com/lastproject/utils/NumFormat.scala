package com.lastproject.utils

object NumFormat {

  def toInt(field:String): Int ={
    try{
      field.toInt
    }catch {
      case _:Exception => 0
    }
  }

  def toDouble(filed:String): Double ={
    try{
      filed.toDouble
    }catch{
      case _:Exception => 0
    }
  }

}
