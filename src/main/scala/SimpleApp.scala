package org.example

import _root_.io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.avro._


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

    val values = stream.map(record => record.value())

//    val s = values.map(gr => {
//      println(gr.toString)
//      val structType = SchemaConverters.toSqlType(gr.getSchema)
//      genericRecordToRow(gr, structType)
//    }).count().foreachRDD(r => {
//      r.foreach(println)
//    })

    values.foreachRDD(rdd => {
      rdd.foreach(gr => {
        println(gr.toString)
      })
    })

    values.print()


//    val spark = SparkSession.builder().config(stream.context.sparkContext.getConf).getOrCreate()
//
//    import spark.implicits._

    streamingContext.start
    streamingContext.awaitTermination
  }

  def genericRecordToRow(record: GenericRecord, sqlType : SchemaConverters.SchemaType): Row = {
    val objectArray = new Array[Any](record.asInstanceOf[GenericRecord].getSchema.getFields.size)
    import scala.collection.JavaConversions._
    for (field <- record.getSchema.getFields) {
      objectArray(field.pos) = record.get(field.pos)
    }

    new GenericRowWithSchema(objectArray, sqlType.dataType.asInstanceOf[StructType])
  }
}

