package org.example

import org.apache.avro.Schema
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.avro._
import org.example.RegistrySchemaProtocol._
import spray.json._
import _root_.io.confluent.kafka.serializers.KafkaAvroDeserializer

import scala.collection.mutable.ListBuffer

object SimpleApp extends App {
  override def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SimpleApp")

    val sparkSession = SparkSession.builder.config(conf).getOrCreate()

    import sparkSession.implicits._

    val schemaStringAsJson = requests.get("http://localhost:8081/subjects/pageviews-value/versions/1").text
      .parseJson


    val schemaFromRegistry = schemaStringAsJson.convertTo[RegistrySchema]
    val avroSchema = new Schema.Parser().parse(schemaFromRegistry.schema)

    val baseline = sparkSession
      .readStream
      .format("kafka")
      .option("subscribe", "pageviews")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .load

    var fieldList = new ListBuffer[String]

    import scala.collection.JavaConversions._
    for (field <- avroSchema.getFields) {
      fieldList += "avro." + field.name
    }

    val columns = fieldList.map(name => new Column(name)).toList

    baseline.printSchema

    println("Schema: " + avroSchema)

    val v = baseline
      .selectExpr("CAST(key AS STRING)", "CAST(value AS BINARY)")
      .select(from_avro($"value", avroSchema.toString) as 'avro)
      .select(columns: _*)

    v.printSchema


    val q = v
      .writeStream
      .queryName("Avro SerDe Test")
      .outputMode("update")
      .format("console")
      .start

    q.awaitTermination
  }
}