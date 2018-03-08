package edu.knoldus

import java.sql.DriverManager

import edu.knoldus.GlobalObject._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils

class PopularHashTagsApplication {

  def main(args: Array[String]) {

    sparkContext.setLogLevel("WARN")

    val url = "jdbc:mysql://localhost:3306/Twitter"
    val username = "root"
    val password = "password"

    val stream = TwitterUtils.createStream(sparkStreamingContext, None)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val hashTagCountOn60sec = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    hashTagCountOn60sec.foreachRDD {
      rdd =>
        rdd.take(3).foreach {
          case (count, hashtag) =>
            Class.forName("com.mysql.jdbc.Driver")
            val conn = DriverManager.getConnection(url, username, password)
            val del = conn.prepareStatement("INSERT INTO HashTagCount (hashtag,count) VALUES (?,?)")
            del.setString(1, hashtag)
            del.setInt(2, count)
            del.executeUpdate
            conn.close()
        }
    }

    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }
}
