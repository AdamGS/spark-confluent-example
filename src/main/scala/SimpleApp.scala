package org.example

import _root_.io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.example.RegistrySchemaProtocol._
import spray.json._



object SimpleApp extends App {
  override def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DeDemo")
    val streamingContext = new StreamingContext(conf, Seconds(3))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "value.deserializer" -> classOf[KafkaAvroDeserializer],
      "key.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "pageviews-consumer-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "schema.registry.url" -> "http://localhost:8081"
    )

    val topics = Array("pageviews")

    val stream = KafkaUtils.createDirectStream[String, GenericRecord](
      streamingContext,
      PreferConsistent,
      Subscribe[String, GenericRecord](topics, kafkaParams)
    )

    stream.map(record => (record.key, record.value)).foreachRDD(rdd => {
      rdd.map({case(_, gr) => new PageView(gr)}).foreach(pv => {
        println("ID is: " + pv.userId + " and the view time is: " + pv.viewTime)
      })
    })

    streamingContext.start
    streamingContext.awaitTermination
  }
}