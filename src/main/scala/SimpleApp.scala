package org.example

import org.apache.avro.Schema
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.avro._
import org.example.RegistrySchemaProtocol._
import spray.json._

import scala.collection.mutable.ListBuffer


object SimpleApp extends App {
  override def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DeDemo")

    val sparkSession = SparkSession.builder.config(conf).getOrCreate()

    import sparkSession.implicits._

    val kafkaParams = Map[String, String](
      "kafka.bootstrap.servers" -> "localhost:9092",
      "schema.registry.url" -> "http://localhost:8081",
      "subscribe" -> "pageviews"
    )

    val schemaStringAsJson = requests.get("http://localhost:8081/subjects/pageviews-value/versions/1").text
      .parseJson

    val schemaFromRegistry = schemaStringAsJson.convertTo[RegistrySchema]
    val schema = new Schema.Parser().parse(schemaFromRegistry.schema)

    val baseline = sparkSession
      .readStream
      .format("kafka")
      .options(kafkaParams)
      .load

    var fieldList = new ListBuffer[String]

    import scala.collection.JavaConversions._
    for (field <- schema.getFields) {
      val newItem = field.name
      fieldList += "value." + newItem
      println(newItem)
    }

    val columns = fieldList.map(name => new Column(name)).toList

    val messagesDF = baseline.withColumn("value", from_avro($"value", schemaFromRegistry.schema))
      .select(columns: _*)

    messagesDF.printSchema

    val q = messagesDF
      .groupBy("userid")
      .count
      .writeStream
      .queryName("Test groupBy")
      .outputMode("update")
      .format("console")
      .start

    q.awaitTermination
  }
}