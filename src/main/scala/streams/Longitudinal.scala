package telemetry.streams

import awscala._
import awscala.s3._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.math.max
import scala.reflect.ClassTag
import telemetry.{DerivedStream, ObjectSummary}
import telemetry.DerivedStream.s3
import telemetry.heka.{HekaFrame, Message}
import telemetry.histograms._
import telemetry.parquet.ParquetFile

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
      value match {
        case h: FlagHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items().booleanType()
        case h: FlagHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().booleanType()
        case h: CountHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items().longType()
        case h: CountHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().longType()
        case h: EnumeratedHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items().array().items().longType()
        case h: EnumeratedHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().array().items().longType()
        case h: BooleanHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items().array().items().longType()
        case h: BooleanHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().array().items().longType()
        case h: LinearHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items(histogramType)
        case h: LinearHistogram =>
          builder.name(key).`type`().optional().map.values().array().items(histogramType)
        case h: ExponentialHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items(histogramType)
        case h: ExponentialHistogram =>
          builder.name(key).`type`().optional().map.values().array().items(histogramType)
        case _ =>
          throw new Exception("Unrecognized histogram type")
      }
    }

    builder.endRecord()
  }


  private def vectorizeHistograms_[T:ClassTag](payloads: List[Map[String, RawHistogram]],
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

  private def vectorizeHistograms[T:ClassTag](name: String,
                                               definition: HistogramDefinition,
                                               payloads: List[Map[String, RawHistogram]],
                                               histogramSchema: Schema): Array[Any] =
    definition match {
      case _: FlagHistogram =>
        vectorizeHistograms_(payloads, name, h => h.values("0") > 0, false)

      case _: BooleanHistogram =>
        def build(h: RawHistogram): Array[Long] = Array(h.values.getOrElse("0", 0L), h.values.getOrElse("1", 0L))
        vectorizeHistograms_(payloads, name, build, Array(0L, 0L))

      case _: CountHistogram =>
        vectorizeHistograms_(payloads, name, h => h.values.getOrElse("0", 0L), 0L)

      case definition: EnumeratedHistogram =>
        def build(h: RawHistogram): Array[Long] = {
          val values = Array.fill(definition.nValues + 1){0L}
          h.values.foreach{case (key, value) =>
            values(key.toInt) = value
          }
          values
        }

        vectorizeHistograms_(payloads, name, build, Array.fill(definition.nValues + 1){0L})

      case definition: LinearHistogram =>
        val buckets = Histograms.linearBuckets(definition.low, definition.high, definition.nBuckets)

        def build(h: RawHistogram): GenericData.Record = {
          val values = Array.fill(buckets.length){0L}
          h.values.foreach{ case (key, value) =>
            val index = buckets.indexOf(key.toInt)
            values(index) = value
          }

          val record = new GenericData.Record(histogramSchema)
          record.put("values", values)
          record.put("sum", h.sum)
          record
        }

        val empty = {
          val record = new GenericData.Record(histogramSchema)
          record.put("values", Array.fill(buckets.length){0L})
          record.put("sum", 0)
          record
        }

        vectorizeHistograms_(payloads, name, build, empty)

      case definition: ExponentialHistogram =>
        val buckets = Histograms.exponentialBuckets(definition.low, definition.high, definition.nBuckets)
        def build(h: RawHistogram): GenericData.Record = {
          val values = Array.fill(buckets.length){0L}
          h.values.foreach{ case (key, value) =>
            val index = buckets.indexOf(key.toInt)
            values(index) = value
          }

          val record = new GenericData.Record(histogramSchema)
          record.put("values", values)
          record.put("sum", h.sum)
          record
        }

        val empty = {
          val record = new GenericData.Record(histogramSchema)
          record.put("values", Array.fill(buckets.length){0L})
          record.put("sum", 0)
          record
        }

        vectorizeHistograms_(payloads, name, build, empty)
    }


  private def buildKeyedHistograms(payloads: List[Map[String, Any]], root: GenericRecordBuilder, schema: Schema) {
    implicit val formats = DefaultFormats

    val histogramsList = payloads.map{ case (x) =>
      val json = x.getOrElse("payload.keyedHistograms", return).asInstanceOf[String]
      parse(json).extract[Map[String, Map[String, RawHistogram]]]
    }

    val uniqueKeys = histogramsList.flatMap(x => x.keys).distinct.toSet

    val validKeys = for {
      key <- uniqueKeys
      definition <- Histograms.definitions.get(key)
    } yield (key, definition)

    val histogramSchema = schema.getField("GC_MS").schema().getTypes()(1).getElementType()

    for ((key, value) <- validKeys) {
      val keyedHistogramsList = histogramsList.map{x =>
        x.get(key) match {
          case Some(x) => x
          case _ => Map[String, RawHistogram]()
        }
      }

      val uniqueLabels = keyedHistogramsList.flatMap(x => x.keys).distinct.toSet
      val vector = vectorizeHistograms(key, value, keyedHistogramsList, histogramSchema)
      val vectorized = for {
        label <- uniqueLabels
        vector = vectorizeHistograms(label, value, keyedHistogramsList, histogramSchema)
      } yield (label, vector)

      root.set(key, vectorized.toMap.asJava)
    }
  }

  private def buildHistograms(payloads: List[Map[String, Any]], root: GenericRecordBuilder, schema: Schema) {
    implicit val formats = DefaultFormats

    val histogramsList = payloads.map{ case (x) =>
      val json = x.getOrElse("payload.histograms", return).asInstanceOf[String]
      parse(json).extract[Map[String, RawHistogram]]
    }

    val uniqueKeys = histogramsList.flatMap(x => x.keys).distinct.toSet

    val validKeys = for {
      key <- uniqueKeys
      definition <- Histograms.definitions.get(key)
    } yield (key, definition)

    val histogramSchema = schema.getField("GC_MS").schema().getTypes()(1).getElementType()

    for ((key, value) <- validKeys) {
      root.set(key, vectorizeHistograms(key, value, histogramsList, histogramSchema))
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

    try {
      buildKeyedHistograms(sorted, root, schema)
      buildHistograms(sorted, root, schema)
    } catch {
      case _ : Throwable =>
        // Ignore buggy clients
        return None
    }

    Some(root.build)
  }
}
