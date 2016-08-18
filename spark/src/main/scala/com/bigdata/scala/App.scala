package com.bigdata.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.commons.io.FileUtils
import java.io.File
import org.mortbay.util.ajax.JSON.Output
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext

object App {
  val HDFS_BASE = "hdfs://localhost:9000/user/bigdata/"
  val INPUT_DATA = "result_2_preparation/4_duplicates/rating"
  val INPUT_USER = "data/user"
  val INPUT_JSON = "data/json"
  val RESULT = "result_3/spark"
  val OUTPUT = "output_spark_jobs/"
  
  val DELIMITER = "\t"
  val USER_DELIMITER = "\\|"
  
  var sc : SparkContext = _
  
  def main(args : Array[String]) {
    try {
      args(0) match {
        case "filter" => filterRatings()
        case "inverted" => buildInvertedIndex(args(1))
        case "sql" => performSQL(args(1))
        case "users" => parseUsers()
        case _ => println("Missing one of these parameters: filter | inverted | sql | users")
      }
    } catch {
      case t: Exception => println(t.getMessage)
    }
  }
  
  def performSQL(titleFilePath: String) = {
    val conf = new SparkConf().setAppName("Spark SQL").setMaster("local")
    sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val ratings = sqlContext.read.json(HDFS_BASE + INPUT_JSON)
    ratings.registerTempTable("ratings")
    val filtered = sqlContext.sql("SELECT movieId FROM ratings WHERE userId = 1 AND rating = 1")
    filtered.show()
    
    // Load info file and join with filtered
    val infoFile = sc.textFile(titleFilePath)
    val titles = infoFile.map(mapperFuncInfo)
    val mapped = filtered.map(x => (x.get(0).toString().toLong, ""))
    val joined = mapped.join(titles).map { case (_, (_, title)) => title}
    
    joined.foreach(println)
  }
  
  def filterRatings() = {
    val conf = new SparkConf().setAppName("Filter out odd ratings").setMaster("local")
    sc = new SparkContext(conf)
    
    val ratingFile = sc.textFile(HDFS_BASE + INPUT_DATA)
    val ratings = ratingFile.map((s : String) => (s.split("\t")(0).toLong, s.split("\t")(1).toLong))
    // filter out records with odd keys
    val filtered = ratings.filter({ case ((key : Long, _)) => (key % 2 == 0) })
    
    filtered.saveAsTextFile(HDFS_BASE + OUTPUT + "even_user_id")
  }
  
  def parseUsers() = {
    // Create SparkContext for local execution
    val conf = new SparkConf().setAppName("Playing with Kryo Serializer").setMaster("local")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[User]))
    sc = new SparkContext(conf)
  
    // Load users and sort in descending order
    val ratingFile = sc.textFile(HDFS_BASE + INPUT_USER)
    val result = ratingFile.map(mapperFuncUser).sortByKey(false)
    result.persist(StorageLevel.MEMORY_ONLY_SER)
    
    // Print the first 5 results
    for (record <- result.take(5)) {
      println("ID: " + record._1 + " : {" + record._2.printUser() + "}")
    }
  }
  
  def buildInvertedIndex(titlesFilePath: String) = {
    // Create SparkContext for local execution
    val conf = new SparkConf().setAppName("Inverted Index with Spark").setMaster("local")
    sc = new SparkContext(conf)
  
    // Load ratings and build Inverted Index using MapReduce operation
    val ratingFile = sc.textFile(HDFS_BASE + INPUT_DATA)
    val invertedIndex = ratingFile.map(mapperFuncMovieUser).reduceByKey(reducerFunc)
    
    // Load info file and join with Inverted Index
    val infoFile = sc.textFile(titlesFilePath)
    val joined = infoFile.map(mapperFuncInfo).join(invertedIndex)
    
    // Format Inverted Index and save as file
    val formatted = joined.sortByKey(true).map(mapperFuncFormat)
    formatted.saveAsTextFile(HDFS_BASE + RESULT)
  }

  def mapperFuncInfo(s: String) : (Long, String) = {
    val parsed = parseRecord(s, DELIMITER)
    return (parsed(0).toLong, parsed(1))
  }
  
  def mapperFuncMovieUser(s: String) : (Long, String) = {
    val parsed = parseRecord(s, DELIMITER)
    return (parsed(1).toLong, parsed(0))
  }

  def mapperFuncFormat(raw : (Long, (String, String))) : String = raw match {
    case (movieId, (movieTitle, userIds)) => movieId + "-" + movieTitle + DELIMITER + userIds
  }
  
  def mapperFuncUser(s: String) : (Long, User) = {
    val parsed = parseRecord(s, USER_DELIMITER)
    return (parsed(0).toLong, new User(parsed(0).toLong, parsed(1).toInt, parsed(2), parsed(3), parsed(4)))
  }
  
  def parseRecord(s: String, del: String) : Array[String] = {
    return s.split(del)
  }
  
  def reducerFunc(v1: String, v2: String) : String = {
    return v1 + "," + v2
  }
  
}
