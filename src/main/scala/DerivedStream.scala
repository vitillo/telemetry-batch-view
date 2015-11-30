package telemetry

import DerivedStream.s3
import awscala.s3._
import com.typesafe.config._
import java.io.File
import java.util.UUID
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.json4s._
import org.json4s.native.JsonMethods._
import scala.collection.JavaConverters._
import scala.io.Source
import telemetry.streams.{E10sExperiment, ExecutiveStream}

case class ObjectSummary(key: String, size: Long) // S3ObjectSummary can't be serialized

abstract class DerivedStream extends java.io.Serializable{
  private val appConf = ConfigFactory.load()
  private val parquetBucket = Bucket(appConf.getString("app.parquetBucket"))
  private val metadataBucket = Bucket("net-mozaws-prod-us-west-2-pipeline-metadata")
  private val metaPrefix = {
    val Some(sourcesObj) = metadataBucket.get(s"sources.json")
    val sources = parse(Source.fromInputStream(sourcesObj.getObjectContent()).getLines().mkString("\n"))
    val JString(metaPrefix) = sources \\ streamName \\ "metadata_prefix"
    metaPrefix
  }

  protected val clsName = DerivedStream.uncamelize(this.getClass.getSimpleName.replace("$", ""))  // Use classname as stream prefix on S3
  protected val partitioning = {
    val Some(schemaObj) = metadataBucket.get(s"$metaPrefix/schema.json")
    val schema = Source.fromInputStream(schemaObj.getObjectContent()).getLines().mkString("\n")
    Partitioning(schema)
  }

  protected def isS3PrefixEmpty(prefix: String): Boolean = {
    s3.objectSummaries(parquetBucket, s"$clsName/$prefix").isEmpty
  }

  protected def uploadLocalFileToS3(path: Path, prefix: String) {
    val uuid = UUID.randomUUID.toString
    val key = s"$clsName/$prefix/$uuid"
    val file = new File(path.toUri())
    val bucketName = parquetBucket.name
    println(s"Uploading Parquet file to $bucketName/$key")
    s3.putObject(bucketName, key, file)
    file.delete()
  }

  protected def streamName: String
  protected def filterPrefix: String = ""
  protected def transform(sc: SparkContext, bucket: Bucket, input: RDD[ObjectSummary], from: String, to: String)
}

object DerivedStream {
  private type OptionMap = Map[Symbol, String]

  implicit val s3: S3 = S3()
  private val metadataBucket = Bucket("net-mozaws-prod-us-west-2-pipeline-metadata")

  private def uncamelize(name: String) = {
    val pattern = java.util.regex.Pattern.compile("(^[^A-Z]+|[A-Z][^A-Z]+)")
    val matcher = pattern.matcher(name);
    val output = new StringBuilder

    while (matcher.find()) {
      if (output.length > 0)
        output.append("-");
      output.append(matcher.group().toLowerCase);
    }

    output.toString()
  }

  private def parseOptions(args: Array[String]): OptionMap = {
    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--from-date" :: value :: tail =>
          nextOption(map ++ Map('fromDate -> value), tail)
        case "--to-date" :: value :: tail =>
          nextOption(map ++ Map('toDate -> value), tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
          nextOption(map ++ Map('stream -> string), list.tail)
        case string :: Nil =>  nextOption(map ++ Map('stream -> string), list.tail)
        case option :: tail => Map()
      }
    }

    nextOption(Map(), args.toList)
  }

  private def S3Prefix(logical: String): String = {
    val Some(sourcesObj) = metadataBucket.get(s"sources.json")
    val sources = parse(Source.fromInputStream(sourcesObj.getObjectContent()).getLines().mkString("\n"))
    val JString(prefix) = sources \\ logical \\ "prefix"
    prefix
  }

  private def convert(stream: String, from: String, to: String) {
    val converter = stream match {
      case "ExecutiveStream" => ExecutiveStream
      case "E10sExperiment" => E10sExperiment("e10s-enabled-aurora-20151020@experiments.mozilla.org",
                                              "telemetry/4/saved_session/Firefox/aurora/43.0a2/")
      case _ => throw new Exception("Stream does not exist!")
    }

    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    val fromDate = formatter.parseDateTime(from)
    val toDate = formatter.parseDateTime(to)
    val daysCount = Days.daysBetween(fromDate, toDate).getDays()
    val bucket = Bucket("net-mozaws-prod-us-west-2-pipeline-data")
    val prefix = S3Prefix(converter.streamName)
    val filterPrefix = converter.filterPrefix

    val conf = new SparkConf().setAppName("telemetry-batch-view")
    val sc = new SparkContext(conf)
    println("Spark parallelism level: " + sc.defaultParallelism)

    val summaries = sc.parallelize(0 until daysCount + 1)
      .map(fromDate.plusDays(_).toString("yyyyMMdd"))
      .flatMap(date => {
                 s3.objectSummaries(bucket, s"$prefix/$date/$filterPrefix")
                   .map(summary => ObjectSummary(summary.getKey(), summary.getSize()))})

    converter.transform(sc, bucket, summaries, from, to)
  }

  def groupBySize(keys: Iterator[ObjectSummary]): List[List[ObjectSummary]] = {
    val threshold = 1L << 31
    keys.foldRight((0L, List[List[ObjectSummary]]()))(
      (x, acc) => {
        acc match {
          case (size, head :: tail) if size + x.size < threshold =>
            (size + x.size, (x :: head) :: tail)
          case (size, res) if size + x.size < threshold =>
            (size + x.size, List(x) :: res)
          case (_, res) =>
            (x.size, List(x) :: res)
        }
      })._2
  }

  def main(args: Array[String]) {
    val usage = "converter --from-date YYYYMMDD --to-date YYYYMMDD stream_name"
    val options = parseOptions(args)

    if (!List('fromDate, 'toDate, 'stream).forall(options.contains)) {
      println(usage)
      return
    }

    convert(options('stream), options('fromDate), options('toDate))
  }
}
