package com.bigdata.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.junit._
import org.junit.Assert._

@Test
class AppTest {
  
  var sc : SparkContext = _
  
  val DEL = App.DELIMITER
  
  // Ratings from user A with ID 10
  val ratingA1 : String = "10" + DEL + "1" + DEL + "2" + DEL + "12345678"
  val ratingA2 : String = "10" + DEL + "2" + DEL + "3" + DEL + "12345678"
  val ratingA3 : String = "10" + DEL + "3" + DEL + "4" + DEL + "12345678"
  // Ratings from user B with ID 20
  val ratingB1 : String = "20" + DEL + "1" + DEL + "2" + DEL + "12345678"
  val ratingB2 : String = "20" + DEL + "2" + DEL + "3" + DEL + "12345678"
  // Ratings from user C with ID 30
  val ratingC1 : String = "30" + DEL + "2" + DEL + "2" + DEL + "12345678"
  
  
  @Before
  def setup() = {
    val conf = new SparkConf().setAppName("App Test").setMaster("local")
    sc = new SparkContext(conf)
  }
  
  @Test
  def invertedBasic = {
    val ratings : Array[String] = Array(ratingA1, ratingA2)
    val testData = sc.parallelize(ratings)
    val invertedIndex = testData.map(App.mapperFuncMovieUser)
      .reduceByKey(App.reducerFunc)
      .sortByKey(true)
    
    assert(invertedIndex.count() == 2, "Lenght of Inverted Index should be 2")
    
    val results = invertedIndex.take(3)    
    assert(results(0)._1 == 1L, "Key of first result is invalid")
    assert(results(0)._2 == "10", "Value of first record is invalid")
    assert(results(1)._1 == 2L, "Key of second result is invalid")
    assert(results(1)._2 == "10", "Value of second record is invalid")
  }
  
  @Test
  def invertedComplex = {
    val ratings : Array[String] = Array(ratingA1, ratingA2,
        ratingA3, ratingB1, ratingB2, ratingC1)
    val testData = sc.parallelize(ratings)
    val invertedIndex = testData.map(App.mapperFuncMovieUser)
      .reduceByKey(App.reducerFunc)
      .sortByKey(true)
    
    assert(invertedIndex.count() == 3, "Lenght of Inverted Index should be 2")
    
    val results = invertedIndex.take(3)    
    assert(results(0)._1 == 1L, "Key of first result is invalid")
    assert(results(0)._2 == "10,20", "Value of first record is invalid")
    assert(results(1)._1 == 2L, "Key of second result is invalid")
    assert(results(1)._2 == "10,20,30", "Value of second record is invalid")
    assert(results(2)._1 == 3L, "Key of third result is invalid")
    assert(results(2)._2 == "10", "Value of third record is invalid")
  }
  
  @After
  def teardown() = {
    sc.stop()
  }
  
}