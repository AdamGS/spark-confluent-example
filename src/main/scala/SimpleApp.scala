import RegistrySchemaProtocol._
import org.apache.avro._
import org.apache.avro.data.Json.ObjectReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import spray.json._

object SimpleApp extends App {
  override def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DeDemo")
    val streamingContext = new StreamingContext(conf, Seconds(3))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "key.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "pageviews-consumer-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("pageviews")

    val stream = KafkaUtils.createDirectStream[String, Array[Byte]](
      streamingContext,
      PreferConsistent,
      Subscribe[String, Array[Byte]](topics, kafkaParams)
    )

    val schemaStringAsJson = requests.get("http://localhost:8081/subjects/pageviews-value/versions/1").text()
      .parseJson
    val schemaFromRegistry = schemaStringAsJson.convertTo[RegistrySchema]
    val schema = new Schema.Parser().parse(schemaFromRegistry.schema)
    val datumReader = new GenericDatumReader[GenericRecord](schema)

    stream.map(record => (record.key, record.value)).foreachRDD(rdd => {
      rdd.foreach(kv => {
        val avroSchema = new Schema.Parser().parse(schemaFromRegistry.schema)
        val jsonReader = new ObjectReader()
        jsonReader.setSchema(avroSchema)
        val decoder = DecoderFactory.get().binaryDecoder(kv._2, null)
        val test = jsonReader.read(null, decoder)
        println(test)
      })
    })

    streamingContext.start
    streamingContext.awaitTermination
  }
}