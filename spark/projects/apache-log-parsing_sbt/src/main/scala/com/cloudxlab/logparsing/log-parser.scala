package com.cloudxlab.logparsing

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class Utils extends Serializable {
  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r

  def containsIP(line: String): Boolean = return line matches "^([0-9\\.]+) .*$"
  //Extract only IP
  def extractIP(line: String): (String) = {
    val pattern = "^([0-9\\.]+) .*$".r
    val pattern(ip: String) = line
    return (ip.toString)
  }

  def parseLogLine(log: String): (String) = {
    val res = PATTERN.findFirstMatchIn(log)
    if (res.isEmpty) {

      //println("Rejected Log Line: " + log)
      val url1 = "";
      return (url1.toString)
      //record_v("Empty", "", "", -1 )
    } else {
      val m = res.get
      val url = m.group(6)

      return (url.toString)
      //record_v(m.group(1), m.group(4),m.group(6), m.group(8).toInt)
    }
  }
  
    def parseLogLine2(log: String): (String) = {
    val res = PATTERN.findFirstMatchIn(log)
    if (res.isEmpty) {

      //println("Rejected Log Line: " + log)
      val url1 = "";
      return (url1.toString)
      //record_v("Empty", "", "", -1 )
    } else {
      val m = res.get
      val url = m.group(4)

      return (url.toString)
      //record_v(m.group(1), m.group(4),m.group(6), m.group(8).toInt)
    }
  }
    def parseLogLine3(log: String): (String) = {
    val res = PATTERN.findFirstMatchIn(log)
    if (res.isEmpty) {

      //println("Rejected Log Line: " + log)
      val url1 = "";
      return (url1.toString)
      //record_v("Empty", "", "", -1 )
    } else {
      val m = res.get
      val url = m.group(8)

      return (url.toString)
      //record_v(m.group(1), m.group(4),m.group(6), m.group(8).toInt)
    }
  }
  def gettop10(accessLogs: RDD[String], sc: SparkContext, topn: Int): Array[(String, Int)] = {
    //Keep only the lines which have IP
    var ipaccesslogs = accessLogs.filter(containsIP)
    var cleanips = ipaccesslogs.map(extractIP(_)).filter(isClassA)
    var ips_tuples = cleanips.map((_, 1));
    var frequencies = ips_tuples.reduceByKey(_ + _);
    var sortedfrequencies = frequencies.sortBy(x => x._2, false)
    return sortedfrequencies.take(topn)
  }
  def isClassA(ip: String): Boolean = {
    ip.split('.')(0).toInt < 127
  }

  def gettop10URL(accessLogs: RDD[String], sc: SparkContext, topnurl: Int): Array[(String, Int)] = {

    // var ipaccesslogs = accessLogs.filter(containsIP)
    var accessLog1 = accessLogs.map(parseLogLine)
    var url_tuples = accessLog1.map((_, 1));
    var frequencies1 = url_tuples.reduceByKey(_ + _);
    var sortedfrequencies = frequencies1.sortBy(x => x._2, false)
    return sortedfrequencies.take(topnurl)
  }

  //gethightraffictop5
  def gethightraffictop5(accessLogs: RDD[String], sc: SparkContext, topnurl: Int): Array[(String, Int)] = {
    var accessLog1 = accessLogs.map(parseLogLine2)
    var url_tuples = accessLog1.map((_, 1));
    var frequencies1 = url_tuples.reduceByKey(_ + _);
    var sortedfrequencies = frequencies1.sortBy(x => x._2, false)
    return sortedfrequencies.take(topnurl)
  }
  //getlowtraffictop5
  def getlowtraffictop5(accessLogs: RDD[String], sc: SparkContext, topnurl: Int): Array[(String, Int)] = {
    var accessLog1 = accessLogs.map(parseLogLine2)
    var url_tuples = accessLog1.map((_, 1));
    var frequencies1 = url_tuples.reduceByKey(_ + _);
    var sortedfrequencies = frequencies1.sortBy(x => x._2, true)
    return sortedfrequencies.take(topnurl)
  }
  
  //gethttpcode
    def gethttpcode(accessLogs: RDD[String], sc: SparkContext, topnurl: Int): Array[(String, Int)] = {
    var accessLog1 = accessLogs.map(parseLogLine3)
    var url_tuples = accessLog1.map((_, 1));
    var frequencies1 = url_tuples.reduceByKey(_ + _);
    var sortedfrequencies = frequencies1.sortBy(x => x._2, true)
    return sortedfrequencies.take(topnurl)
  }

}
object EntryPoint {
    val usage = """
        Usage: EntryPoint <how_many> <file_or_directory_in_hdfs>
        Eample: EntryPoint 10 /data/spark/project/access/access.log.45.gz
    """
    
    def main(args: Array[String]) {
        
        if (args.length != 3) {
            println("Expected:3 , Provided: " + args.length)
            println(usage)
            return;
        }

        var utils = new Utils

        // Create a local StreamingContext with batch interval of 10 second
        val conf = new SparkConf().setAppName("WordCount")
        val sc = new SparkContext(conf);
        sc.setLogLevel("WARN")

        // var accessLogs = sc.textFile("/data/spark/project/access/access.log.45.gz")
        var accessLogs = sc.textFile(args(2))
        val top10 = utils.gettop10(accessLogs, sc, args(1).toInt)
        println("===== TOP 10 IP Addresses =====")
        for(i <- top10){
            println(i)
        }
        
        val urltop10 = utils.gettop10URL(accessLogs, sc, args(1).toInt)
          println("===== TOP 10 URL =====")
           for(i <- urltop10){
            println(i)
           }
        
         val hightraffictop5 = utils.gethightraffictop5(accessLogs, sc, args(1).toInt)
          println("===== TOP 5 time frames for high traffic =====")
           for(i <- hightraffictop5){
            println(i)
        }
            
                  
             val lowtraffictop5 = utils.getlowtraffictop5(accessLogs, sc, args(1).toInt)
          println("===== TOP 5 time frames for low traffic =====")
           for(i <- lowtraffictop5){
            println(i)
           }
                        val httpcodecnt = utils.gethttpcode(accessLogs, sc, args(1).toInt)
          println("===== Find HTTP codes=====")
           for(i <- httpcodecnt){
            println(i)
           }
}
}