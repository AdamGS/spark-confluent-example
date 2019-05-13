package org.example

import org.apache.avro.generic.GenericRecord

case class PageView(viewTime: Long, userId: String, pageId: String) {
  def this(gr: GenericRecord)  {
    this(
      gr.get("viewtime").asInstanceOf[Long],
      gr.get("userid").toString,
      gr.get("pageid").toString)
  }
}