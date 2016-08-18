package com.bigdata.scala

class User(useridc : Long, agec : Int, genderc : String, occupationc : String, zipc : String) {
  val userid = useridc
  val age = agec
  val gender = genderc
  val occupation = occupationc
  val zip = zipc
  
  def printUser() : String = {
    return "userid:" + userid + ", age:" + age + ", gender:" + gender + ", occupation:" + occupation + ", zip:" + zip
  }
}