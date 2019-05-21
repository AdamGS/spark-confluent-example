name := "Simple Project"

version := "1.0"

scalaVersion := "2.12.8"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10-assembly" % "2.4.3"
libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"
libraryDependencies += "com.lihaoyi" %% "requests" % "0.1.7"
libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "2.0.4"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.5"
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "5.2.1"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.8"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.8"




