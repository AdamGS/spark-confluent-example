package org.example

import spray.json.DefaultJsonProtocol

case class RegistrySchema(subject:String, version: Int, id: Int, schema: String)

object RegistrySchemaProtocol extends DefaultJsonProtocol {
  implicit val schemaFormat = jsonFormat4(RegistrySchema)
}
