package edu.knoldus

import java.sql.DriverManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.streaming.twitter.TwitterUtils

object PopularHashTags {


  val conf = new SparkConf().setMaster("local[4]").setAppName("spark-Streaming-popularHashTags")
  val sc = new SparkContext(conf)


  def main(args: Array[String]) {

    sc.setLogLevel("WARN")

    // Set the Spark StreamingContext to create a DStream for every 5 seconds
    val ssc = new StreamingContext(sc, Seconds(30))
    // Pass the filter keywords as arguements

    //  val stream = FlumeUtils.createStream(ssc, args(0), args(1).toInt)
    val stream = TwitterUtils.createStream(ssc, None)

    // Split the stream on space and extract hashtags
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // Get the top hashtags over the previous 10 sec window
    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // print tweets in the currect DStream
    stream.print()

    val url = "jdbc:mysql://localhost:3306/Twitter"
    val username = "root"
    val password = "password"

    topCounts10.foreachRDD {
      rdd =>
        rdd.take(3).foreach{
          case (count,hashtag) =>
            Class.forName("com.mysql.jdbc.Driver")
            val conn = DriverManager.getConnection(url, username, password)
            val del = conn.prepareStatement("INSERT INTO HashTagCount (hashtag,count) VALUES (?,?)")
              del.setString(1, hashtag)
              del.setInt(2, count)
              del.executeUpdate
            conn.close()
        }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
