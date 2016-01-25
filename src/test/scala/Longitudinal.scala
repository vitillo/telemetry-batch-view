package telemetry.test

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import telemetry.streams.Longitudinal
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericData, GenericRecordBuilder}
import org.apache.avro.generic.GenericData.Record

class LongitudinalTest extends FlatSpec with Matchers with PrivateMethodTester{
  def fixture = {
    def createPayload(creationTimestamp: Double): Map[String, Any] = {
      val histograms =
        ("TELEMETRY_TEST_FLAG" ->
           ("values" -> ("0" -> 1) ~ ("1" -> 0)) ~
           ("sum" -> 0)) ~
        ("DEVTOOLS_TOOLBOX_OPENED_BOOLEAN" ->
           ("values" -> ("0" -> 42)) ~
           ("sum" -> 0)) ~
        ("UPDATE_CHECK_NO_UPDATE_EXTERNAL" ->
           ("values" -> ("0" -> 42)) ~
           ("sum" -> 42))


      Map("clientId" -> "26c9d181-b95b-4af5-bb35-84ebf0da795d",
          "creationTimestamp" -> creationTimestamp,
          "os" -> "Windows_NT",
          "payload.histograms" -> compact(render(histograms)))
    }

    new {
      private val view = Longitudinal()

      private val buildSchema = PrivateMethod[Schema]('buildSchema)
      private val buildRecord = PrivateMethod[Option[GenericRecord]]('buildRecord)

      val schema = view invokePrivate buildSchema()
      val payloads = for (i <- 1 to 10) yield createPayload(i.toDouble)
      val record = (view invokePrivate buildRecord(payloads.toIterable, schema)).get
    }
  }

  "Top-level measurements" must "be stored correctly" in {
    assert(fixture.record.get("clientId") == fixture.payloads(0)("clientId"))
    assert(fixture.record.get("os") == fixture.payloads(0)("os"))
  }

  "creationTimestamp" must "be stored correctly" in {
    val creationTimestamps = fixture.record.get("creationTimestamp").asInstanceOf[Array[Double]].toList
    assert(creationTimestamps.length == fixture.payloads.length)
    creationTimestamps.zip(fixture.payloads.map(_("creationTimestamp"))).foreach{case (x, y) => assert(x == y)}
  }

  "Flag histograms" must "be stored correctly" in {
    val histograms = fixture.record.get("TELEMETRY_TEST_FLAG").asInstanceOf[Array[Boolean]].toList
    assert(histograms.length == fixture.payloads.length)
    histograms.zip(Stream.continually(true)).foreach{case (x, y) => assert(x == y)}
  }

  "Boolean histograms" must "be stored correctly" in {
    val histograms = fixture.record.get("DEVTOOLS_TOOLBOX_OPENED_BOOLEAN").asInstanceOf[Array[Array[Long]]].toList
    assert(histograms.length == fixture.payloads.length)
    histograms.zip(Stream.continually(Array(42L, 0L))).foreach{case (x, y) => assert(x.toList == y.toList)}
  }

  "Count histograms" must "be stored correctly" in {
    val histograms = fixture.record.get("UPDATE_CHECK_NO_UPDATE_EXTERNAL").asInstanceOf[Array[Long]].toList
    assert(histograms.length == fixture.payloads.length)
    histograms.zip(Stream.continually(42)).foreach{case (x, y) => assert(x== y)}
  }
}
