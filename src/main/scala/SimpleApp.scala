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
      .setAppName("SimpleApp")

    val sparkSession = SparkSession.builder.config(conf).getOrCreate()

    import sparkSession.implicits._

    val schemaStringAsJson = requests.get("http://localhost:8081/subjects/pageviews-value/versions/latest").text.parseJson

    val directURLSchema = requests.get("http://localhost:8081/subjects/pageviews-value/versions/latest/schema")
    println("New! " + directURLSchema)



    val schemaFromRegistry = schemaStringAsJson.convertTo[RegistrySchema]
    val avroSchema = new Schema.Parser().parse(directURLSchema.text)

    val baseline = sparkSession
      .readStream
      .format("kafka")
      .option("subscribe", "pageviews")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .load

    var fieldList = new ListBuffer[String]

    val columns = fieldList.map(name => new Column(name)).toList

    baseline.printSchema
    println("Schema: " + avroSchema.toString)

    val v = baseline
      .selectExpr("CAST(key AS STRING)", "value")
      .select($"key", $"value", from_avro($"value", avroSchema.toString) as 'pageviews)
      .select($"key", $"value"cast("STRING"), $"pageviews.*")

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