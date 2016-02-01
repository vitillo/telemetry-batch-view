package telemetry.test

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import telemetry.streams.Longitudinal
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericData, GenericRecordBuilder}
import org.apache.avro.generic.GenericData.Record
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import telemetry.parquet.ParquetFile

class LongitudinalTest extends FlatSpec with Matchers with PrivateMethodTester{
  def fixture = {
    def createPayload(creationTimestamp: Double): Map[String, Any] = {
      // TODO: Use Scala Map and List directly?
      val histograms =
        ("TELEMETRY_TEST_FLAG" ->
           ("values" -> ("0" -> 1) ~ ("1" -> 0)) ~
           ("sum" -> 0)) ~
        ("DEVTOOLS_TOOLBOX_OPENED_BOOLEAN" ->
           ("values" -> ("0" -> 42)) ~
           ("sum" -> 0)) ~
        ("UPDATE_CHECK_NO_UPDATE_EXTERNAL" ->
           ("values" -> ("0" -> 42)) ~
           ("sum" -> 42)) ~
        ("PLACES_BACKUPS_DAYSFROMLAST" ->
           ("values" -> ("1" -> 42)) ~
           ("sum" -> 42)) ~
        ("GC_BUDGET_MS" ->
           ("values" -> ("1" -> 42)) ~
           ("sum" -> 42)) ~
        ("GC_MS" ->
           ("values" -> ("1" -> 42)) ~
           ("sum" -> 42))

      val keyedHistograms =
        ("ADDON_SHIM_USAGE" ->
           ("foo" ->
             ("values" -> ("1" -> 42)) ~
             ("sum" -> 42))) ~
        ("SEARCH_COUNTS" ->
           ("foo" ->
              ("values" -> ("0" -> 42)) ~
              ("sum" -> 42))) ~
        ("DEVTOOLS_PERFTOOLS_SELECTED_VIEW_MS" ->
           ("foo" ->
              ("values" -> ("1" -> 42)) ~
              ("sum" -> 42)))

      val system =
          ("cpu" ->
             ("count" -> 4)) ~
          ("os" ->
             ("name" -> "Windows_NT") ~
             ("locale" -> "en_US") ~
             ("version" -> "6.1")) ~
          ("hdd" ->
             ("profile" ->
                ("revision" -> "12345") ~
                ("model" -> "SAMSUNG X"))) ~
          ("gfx" ->
             ("adapters" -> List(
                ("RAM" -> 1024) ~ ("description" -> "FOO") ~ ("deviceID" -> "1") ~ ("vendorID" -> "Vendor") ~ ("GPUActive" -> true),
                ("RAM" -> 1024) ~ ("description" -> "FOO") ~ ("deviceID" -> "1") ~ ("vendorID" -> "Vendor") ~ ("GPUActive" -> true)
              )))

      val settings =
        ("e10sEnabled" -> true)

      val build =
        ("buildId" -> "20160101001100")

      Map("clientId" -> "26c9d181-b95b-4af5-bb35-84ebf0da795d",
          "creationTimestamp" -> creationTimestamp,
          "os" -> "Windows_NT",
          "payload.histograms" -> compact(render(histograms)),
          "payload.keyedHistograms" -> compact(render(keyedHistograms)),
          "environment.system" -> compact(render(system)),
          "environment.settings" -> compact(render(settings)),
          "environment.build" -> compact(render(build)))
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


  "Records" can "be serialized" in {
    ParquetFile.serialize(List(fixture.record).toIterator, fixture.schema)
  }

  "environment.system" must "be converted correctly" in {
    val records = fixture.record.get("system").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("cpu").asInstanceOf[Record].get("count") == 4)
    }
  }

  "environment.settings" must "be converted correctly" in {
    val records = fixture.record.get("settings").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("e10sEnabled") == true)
    }
  }

  "environment.build" must "be converted correctly" in {
    val records = fixture.record.get("build").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("buildId") == "20160101001100")
    }
  }

   "Top-level measurements" must "be converted correctly" in {
    assert(fixture.record.get("clientId") == fixture.payloads(0)("clientId"))
    assert(fixture.record.get("os") == fixture.payloads(0)("os"))
  }

  "creationTimestamp" must "be converted correctly" in {
    val creationTimestamps = fixture.record.get("creationTimestamp").asInstanceOf[Array[Double]].toList
    assert(creationTimestamps.length == fixture.payloads.length)
    creationTimestamps.zip(fixture.payloads.map(_("creationTimestamp"))).foreach{case (x, y) => assert(x == y)}
  }

  "Flag histograms" must "be converted correctly" in {
    val histograms = fixture.record.get("TELEMETRY_TEST_FLAG").asInstanceOf[Array[Any]].toList
    assert(histograms.length == fixture.payloads.length)
    histograms.zip(Stream.continually(true)).foreach{case (x, y) => assert(x == y)}
  }

  "Boolean histograms" must "be converted correctly" in {
    val histograms = fixture.record.get("DEVTOOLS_TOOLBOX_OPENED_BOOLEAN").asInstanceOf[Array[Any]].toList
    assert(histograms.length == fixture.payloads.length)
    histograms.foreach(h => assert(h.asInstanceOf[Array[Long]].toList == List(42L, 0L)))
  }

  "Count histograms" must "be converted correctly" in {
    val histograms = fixture.record.get("UPDATE_CHECK_NO_UPDATE_EXTERNAL").asInstanceOf[Array[Any]].toList
    assert(histograms.length == fixture.payloads.length)
    histograms.zip(Stream.continually(42)).foreach{case (x, y) => assert(x== y)}
  }

  "Enumerated histograms" must "be converted correctly" in {
    val histograms = fixture.record.get("PLACES_BACKUPS_DAYSFROMLAST").asInstanceOf[Array[Any]]
    assert(histograms.length == fixture.payloads.length)
    for(h <- histograms) {
      val histogram = h.asInstanceOf[Array[Long]]
      assert(histogram.length == 16)

      for((value, key) <- histogram.zipWithIndex) {
        if (key == 1)
          assert(value == 42)
        else
          assert(value == 0)
      }
    }
  }

  "Linear histograms" must "be converted correctly" in {
    val records = fixture.record.get("GC_BUDGET_MS").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)

    val reference = Array(0L, 42L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)
    records.foreach{ x =>
      val tmp = x.asInstanceOf[Record]
      assert(tmp.get("sum") == 42L)
      assert(tmp.get("values").asInstanceOf[Array[Long]].toList == reference.toList)
    }
  }

  "Exponential histograms" must "be converted correctly" in {
    val records = fixture.record.get("GC_MS").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)

    val reference = Array.fill(50){0}
    reference(1) = 42

    records.foreach{ x =>
      val tmp = x.asInstanceOf[Record]
      assert(tmp.get("sum") == 42L)
      assert(tmp.get("values").asInstanceOf[Array[Long]].toList == reference.toList)
    }
  }

  "Keyed enumerated histograms" must "be converted correctly" in {
    // Keyed boolean histograms follow a similar structure
    val records = fixture.record.get("ADDON_SHIM_USAGE").asInstanceOf[java.util.Map[String, Array[Any]]].asScala
    assert(records.size == 1)

    for(h <- records("foo")) {
      val histogram = h.asInstanceOf[Array[Long]]
      assert(histogram.length == 16)

      for((value, key) <- histogram.zipWithIndex) {
        if (key == 1)
          assert(value == 42)
        else
          assert(value == 0)
      }
    }
  }

  "Keyed count histograms" must "be converted correctly" in {
    // Keyed flag histograms follow a similar structure
    val searchCounts = fixture.record.get("SEARCH_COUNTS").asInstanceOf[java.util.Map[String, Array[Any]]].asScala
    assert(searchCounts.size == 1)

    val histograms = searchCounts("foo")
    assert(histograms.length == fixture.payloads.length)

    histograms.zip(Stream.continually(42)).foreach{case (x, y) => assert(x== y)}
  }

  "Keyed exponential histograms" must "be converted correctly" in {
    // Keyed linear histograms follow a similar structure
    val records = fixture.record.get("DEVTOOLS_PERFTOOLS_SELECTED_VIEW_MS").asInstanceOf[java.util.Map[String, Array[Any]]].asScala
    assert(records.size == 1)

    val histograms = records("foo")
    val reference = Array.fill(20){0}
    reference(1) = 42

    histograms.foreach{ x =>
      val tmp = x.asInstanceOf[Record]
      assert(tmp.get("sum") == 42L)
      assert(tmp.get("values").asInstanceOf[Array[Long]].toList == reference.toList)
    }
  }
}
