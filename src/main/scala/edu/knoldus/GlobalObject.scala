package edu.knoldus

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object GlobalObject {

  val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("spark-Streaming-popularHashTags")
  val sparkContext = new SparkContext(sparkConf)
  val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(30))

}
