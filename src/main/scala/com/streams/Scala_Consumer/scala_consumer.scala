package com.streams.Scala_Consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{ DStream, InputDStream, ConstantInputDStream }
import org.apache.spark.streaming.kafka.v09.KafkaUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.sql.functions.avg
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.producer._
import org.apache.kafka.common.serialization.{ Deserializer, Serializer }
import org.apache.kafka.common.serialization.StringSerializer;

object scala_consumer extends Serializable {

  // schema for sensor data   
  /*case class Sensor(resid: String, date: String, time: String, hz: Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double) extends Serializable

  // function to parse line of sensor data into Sensor class
  def parseSensor(str: String): Sensor = {
    val p = str.split(",")
    Sensor(p(0), p(1), p(2), p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
  }*/
  val timeout = 10 // Terminate after N seconds
  val batchSeconds = 2 // Size of batch intervals

  def main(args: Array[String]): Unit = {

    val brokers = "maprdemo:9092" // not needed for MapR Streams, needed for Kafka
    val groupId = "testgroup"
    val offsetReset = "earliest"
    val batchInterval = "2"
    val pollTimeout = "1000"
    val topics = "/user/vipulrajan/streaming/original:sensor"
    val topica = "/user/vipulrajan/streaming/fail:test"

    val sparkConf = new SparkConf().setAppName("SensorStream").setMaster("local[1]").set("spark.testing.memory", "536870912")
                                    .set("spark.streaming.backpressure.enabled", "true")
	                                  .set("spark.streaming.receiver.maxRate", Integer.toString(2000000))
	                                  .set("spark.streaming.kafka.maxRatePerPartition", Integer.toString(2000000));

    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval.toInt))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      "spark.kafka.poll.time" -> pollTimeout
    )

    val producerConf = new ProducerConf(
      bootstrapServers = brokers.split(",").toList
    )

    val messages = KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, topicsSet)

    val values: DStream[String] = messages.map(_._2)
    println("message values received")
    values.print(10)

    val alertsDStream: DStream[String] = values.filter(_.split(",")(3).toDouble == 0)

    println("filtered alert messages ")
    alertsDStream.print(10)

    alertsDStream.sendToKafka[StringSerializer](topica,producerConf)   


    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }
}