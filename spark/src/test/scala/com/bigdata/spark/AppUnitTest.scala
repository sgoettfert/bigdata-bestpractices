package com.bigdata.spark

import org.junit._
import org.junit.Assert._

@Test
class AppUnitTest {

    val ratingRecord = 1 + App.DELIMITER + 2 + App.DELIMITER + 3 + App.DELIMITER + 12345678
    val infoRecord = "1" + App.DELIMITER + "MovieTitle"
    
    @Test
    def testmapperFuncInfo() = {
      val result = App.mapperFuncInfo(infoRecord)
      assert(result == (1L, "MovieTitle"), "Function 'mapperFuncInfo' failed")
    }
    
    @Test
    def testMapperFuncMovieUser() = {
      val result = App.mapperFuncMovieUser(ratingRecord)
      assert(result == (2L, "1"), "Function 'mapperFuncMovieUser' failed")
    }
    
    @Test
    def testMapperFormat() = {
      val testRow = (1L, ("Title", "2,3,4"))
      val result = App.mapperFuncFormat(testRow)
      assert(result == "1-Title" + App.DELIMITER + "2,3,4", "Function 'mapperFuncFormat' failed")
    }
    
    @Test
    def testMapperFuncUser() = {
      val testString = "1|24|M|technician|85711"
      val result = App.mapperFuncUser(testString)
    }
    
    @Test
    def testParseInfo() = {
      val result = App.parseRecord(infoRecord, App.DELIMITER)
      assert(result.deep == Array("1", "MovieTitle").deep, "Function 'parseInfo' failed")
    }
    
    @Test
    def testParseRating() = {
      val result = App.parseRecord(ratingRecord, App.DELIMITER)
      assert(result.deep == Array("1", "2", "3", "12345678").deep, "Function 'parseRating' failed")
    }
    
    @Test
    def testReducer() = {
      val testS1 = "1"; val testS2 = "2"
      val result = App.reducerFunc(testS1, testS2)
      assert(result == "1,2", "Function 'reducerFunc' failed")
    }
    
}


