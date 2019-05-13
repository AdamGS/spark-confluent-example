name := "Simple Project"

version := "1.0"

scalaVersion := "2.12.8"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.2"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "2.4.2"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10-assembly_2.12" % "2.4.2"
libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"
libraryDependencies += "com.lihaoyi" %% "requests" % "0.1.7"
libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "2.0.4"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.5"
libraryDependencies += "com.lihaoyi" %% "upickle" % "0.7.4"
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "5.2.1"



