package telemetry.streams

import awscala._
import awscala.s3._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericData, GenericRecordBuilder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._
import telemetry.{DerivedStream, ObjectSummary}
import telemetry.DerivedStream.s3
import telemetry.heka.{HekaFrame, Message}
import telemetry.parquet.ParquetFile
import telemetry.histograms.{Histograms, RawHistogram}
import scala.util.Random
import collection.JavaConversions._
import scala.math.max
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

case class Longitudinal() extends DerivedStream {
  override def streamName: String = "telemetry-release"
  override def filterPrefix: String = "telemetry/4/main/Firefox/release/*/*/*/42/"

  override def transform(sc: SparkContext, bucket: Bucket, summaries: RDD[ObjectSummary], from: String, to: String) {
    val prefix = s"generationDate=$to"

    if (!isS3PrefixEmpty(prefix)) {
      println(s"Warning: prefix $prefix already exists on S3!")
      return
    }

    val groups = DerivedStream.groupBySize(summaries.collect().toIterator)
    val clientMessages = sc.parallelize(groups, groups.size)
      .flatMap(x => x)
      .flatMap{ case obj =>
        val hekaFile = bucket.getObject(obj.key).getOrElse(throw new Exception("File missing on S3"))
        for (message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey()))  yield message }
      .flatMap{ case message =>
        val fields = HekaFrame.fields(message)
        val clientId = fields.get("clientId")

        clientId match {
          case Some(client: String) => List((client, fields))
          case _ => Nil
        }}
      .groupByKey()
      .coalesce(max((0.5*sc.defaultParallelism).toInt, 1), true)  // see https://issues.apache.org/jira/browse/PARQUET-222

    clientMessages
      .values
      .foreachPartition{ case clientIterator =>
        val schema = buildSchema
        val records = for {
          client <- clientIterator
          record <- buildRecord(client, schema)
        } yield record

        while(!records.isEmpty) {
          val localFile = ParquetFile.serialize(records, schema)
          uploadLocalFileToS3(localFile, prefix)
        }
    }
  }

  private def buildSchema: Schema = {
    val builder = SchemaBuilder
      .record("Submission")
      .fields
      .name("clientId").`type`().stringType().noDefault()
      .name("creationTimestamp").`type`().array().items().doubleType().noDefault()
      .name("os").`type`().stringType().noDefault()
      .name("simpleMeasurements").`type`().array().items().stringType().noDefault()
      .name("log").`type`().array().items().stringType().noDefault()
      .name("info").`type`().array().items().stringType().noDefault()
      .name("addonDetails").`type`().array().items().stringType().noDefault()
      .name("settings").`type`().array().items().stringType().noDefault()
      .name("profile").`type`().array().items().stringType().noDefault()
      .name("build").`type`().array().items().stringType().noDefault()
      .name("partner").`type`().array().items().stringType().noDefault()
      .name("system").`type`().array().items().stringType().noDefault()

    val histogramType = SchemaBuilder
      .record("Histogram")
      .fields()
      .name("values").`type`().array().items().longType().noDefault()
      .name("sum").`type`().longType().noDefault()
      .endRecord()

    // TODO: add description to histograms
    Histograms.definitions.foreach{ case (key, value) =>
      (value.kind, value.keyed.getOrElse(false)) match {
        case ("flag", false) =>
          builder.name(key).`type`().optional().array().items().booleanType()
        case ("flag", true) =>
          builder.name(key).`type`().optional().array().items().map().values().booleanType()
        case ("count", false) =>
          builder.name(key).`type`().optional().array().items().longType()
        case ("count", true) =>
          builder.name(key).`type`().optional().array().items().map().values().longType()
        case ("boolean", false) =>
          builder.name(key).`type`().optional().array().items(histogramType)
        case ("boolean", true) =>
          builder.name(key).`type`().optional().array().items().map().values(histogramType)
        case ("enumerated", false) =>
          builder.name(key).`type`().optional().array().items().array().items().longType()
        case ("enumerated", true) =>
          builder.name(key).`type`().optional().array().items().map().values().array().items().longType()
        case ("linear", false) =>
          builder.name(key).`type`().optional().array().items(histogramType)
        case ("linear", true) =>
          builder.name(key).`type`().optional().array().items().map().values(histogramType)
        case ("exponential", false) =>
          builder.name(key).`type`().optional().array().items(histogramType)
        case ("exponential", true) =>
          builder.name(key).`type`().optional().array().items().map().values(histogramType)
        case _ =>
          throw new Exception("Unrecognized histogram type")
      }
    }

    builder.endRecord()
  }

  private def vectorizeHistograms[T:ClassTag](payloads: List[Map[String, RawHistogram]],
                                              name: String,
                                              builder: RawHistogram => T,
                                              default: T): Array[T] = {
    val buffer = ListBuffer[T]()
    for (histograms <- payloads) {
      histograms.get(name) match {
        case Some(histogram) =>
          buffer += builder(histogram)
        case None =>
          buffer += default
      }
    }
    buffer.toArray
  }

  private def buildHistograms(payloads: List[Map[String, Any]], root: GenericRecordBuilder, schema: Schema) {
    implicit val formats = DefaultFormats

    val histogramsList = payloads.map{ case (x) =>
      val json = x("payload.histograms").asInstanceOf[String]
      parse(json).extract[Map[String, RawHistogram]]
    }

    val uniqueKeys = histogramsList.flatMap(x => x.keys).distinct.toSet

    val validKeys = for {
      key <- uniqueKeys
      definition <- Histograms.definitions.get(key)
    } yield (key, definition)

    val histogramSchema = schema.getField("GC_MS").schema().getTypes()(1).getElementType()

    for ((key, definition) <- validKeys) {
      definition.kind match {
        case "flag" =>
          root.set(key, vectorizeHistograms(histogramsList, key, h => h.values("0") > 0, false))

        case "boolean" =>
          def build(h: RawHistogram): GenericData.Record = {
            val record = new GenericData.Record(histogramSchema)
            val values = Array(h.values.getOrElse("0", 0L), h.values.getOrElse("1", 0L))
            val sum = h.sum

            record.put("values", values)
            record.put("sum", sum)
            record
          }

          val empty = {
            val record = new GenericData.Record(histogramSchema)
            record.put("values", Array[Long](0L, 0L))
            record.put("sum", 0)
            record
          }

          root.set(key, vectorizeHistograms(histogramsList, key, build, empty))

        case "count" =>
          root.set(key, vectorizeHistograms(histogramsList, key, h => h.values.getOrElse("0", 0L), 0L))

        case "enumerated" =>
          definition.n_values match {
            case Some(nBins) =>

              def build(h: RawHistogram): Array[Long] = {
                val values = Array.fill(nBins + 1){0L}
                h.values.foreach{case (key, value) =>
                  values(key.toInt) = value
                }
                values
              }

              root.set(key, vectorizeHistograms(histogramsList, key, build, Array.fill(nBins + 1){0L}))

            case _ =>
              // Ignore invalid histogram definition
          }

        case _ =>
      }
    }
  }

  private def buildRecord(history: Iterable[Map[String, Any]], schema: Schema): Option[GenericRecord] = {
    // Sort records by timestamp
    val sorted = history
      .toList
      .sortWith((x, y) => {
                 (x("creationTimestamp"), y("creationTimestamp")) match {
                   case (creationX: Double, creationY: Double) =>
                     creationX < creationY
                   case _ =>
                     return None  // Ignore 'unsortable' client
                 }
               })

    val root = new GenericRecordBuilder(schema)
      .set("clientId", sorted(0)("clientId").asInstanceOf[String])
      .set("os", sorted(0)("os").asInstanceOf[String])
      .set("creationTimestamp", sorted.map(x => x("creationTimestamp").asInstanceOf[Double]).toArray)
      .set("simpleMeasurements", sorted.map(x => x.getOrElse("payload.simpleMeasurements", "").asInstanceOf[String]).toArray)
      .set("log", sorted.map(x => x.getOrElse("payload.log", "").asInstanceOf[String]).toArray)
      .set("info", sorted.map(x => x.getOrElse("payload.info", "").asInstanceOf[String]).toArray)
      .set("addonDetails", sorted.map(x => x.getOrElse("payload.addonDetails", "").asInstanceOf[String]).toArray)
      .set("settings", sorted.map(x => x.getOrElse("environment.settings", "").asInstanceOf[String]).toArray)
      .set("profile", sorted.map(x => x.getOrElse("environment.profile", "").asInstanceOf[String]).toArray)
      .set("build", sorted.map(x => x.getOrElse("environment.build", "").asInstanceOf[String]).toArray)
      .set("partner", sorted.map(x => x.getOrElse("environment.partner", "").asInstanceOf[String]).toArray)
      .set("system", sorted.map(x => x.getOrElse("environment.system", "").asInstanceOf[String]).toArray)

    buildHistograms(sorted, root, schema)
    Some(root.build)
  }
}
