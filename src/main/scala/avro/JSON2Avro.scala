package telemetry.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object JSON2Avro{
  def parseRecord(schema: Schema, json: JValue): Option[GenericData.Record] = {
    val record = new GenericData.Record(schema)

    for (field <- schema.getFields) {
      val parsed = parse(field.schema, json \ field.name)
      parsed match {
        case Some(value) =>
          record.put(field.name, value)
        case _ =>
      }
    }

    Some(record)
  }

  def parseArray(schema : Schema, json: JValue): Option[Array[Any]] = json match {
    case JArray(json) =>
      val parsed = for { elem <- json } yield {
        parse(schema.getElementType(), elem)
      }
      Some(parsed.flatten.toArray)
    case _ =>
      None
  }

  def parseString(schema: Schema, json: JValue): Option[String] = json match {
    case JString(value) =>
      Some(value)
    case _ =>
      None
  }

  def parseInt(Schema: Schema, json: JValue): Option[Int] = json match {
    case JInt(value) =>
      Some(value.toInt)
    case _ =>
      None
  }

  def parseBoolean(Schema: Schema, json: JValue): Option[Boolean] = json match {
    case JBool(value) =>
      Some(value)
    case _ =>
      None
  }

  def parse(schema: Schema, json: JValue): Option[Any] = schema.getType() match{
    case Schema.Type.RECORD =>
      parseRecord(schema, json)

    case Schema.Type.ARRAY =>
      parseArray(schema, json)

    case Schema.Type.STRING =>
      parseString(schema, json)

    case Schema.Type.UNION =>
      parse(schema.getTypes()(1), json)

    case Schema.Type.INT =>
      parseInt(schema, json)

    case Schema.Type.BOOLEAN =>
      parseBoolean(schema, json)

    case _ =>
      throw new Exception("Unhandled type")
  }
}

