package telemetry.histograms

import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.{Map => MMap}
import scala.io.Source

case class RawHistogram(values: Map[String, Long], sum: Long)

case class HistogramDefinition(kind: String,
                               keyed: Boolean,
                               nValues: Option[Int],
                               low: Option[Int],
                               high: Option[Int],
                               nBuckets: Option[Int])

object Histograms {
  val definitions = {
    implicit val formats = DefaultFormats

    val uris = Map("release" -> "https://hg.mozilla.org/releases/mozilla-release/raw-file/tip/toolkit/components/telemetry/Histograms.json",
                   "beta" -> "https://hg.mozilla.org/releases/mozilla-beta/raw-file/tip/toolkit/components/telemetry/Histograms.json",
                   "aurora" -> "https://hg.mozilla.org/releases/mozilla-aurora/raw-file/tip/toolkit/components/telemetry/Histograms.json",
                   "nightly" -> "https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/Histograms.json")

    val parsed = uris.map{ case (key, value) =>
      val json = parse(Source.fromURL(value, "UTF8").mkString)
      val result = MMap[String, MMap[String, Option[Any]]]()

      /* Unfortunately the histogram definition file does not respect a proper schema and
         as such it's rather unpleasant to parse it in a statically typed langauge... */

      for {
        JObject(root) <- json
        JField(name, JObject(histogram)) <- root
        JField(k, v) <- histogram
      } yield {
        val value = try {
          (k, v) match {
            case ("low", JString(v)) => Some(v.toInt)
            case ("low", JInt(v)) => Some(v.toInt)
            case ("high", JString(v)) => Some(v.toInt)
            case ("high", JInt(v)) => Some(v.toInt)
            case ("n_buckets", JString(v)) => Some(v.toInt)
            case ("n_buckets", JInt(v)) => Some(v.toInt)
            case ("n_values", JString(v)) => Some(v.toInt)
            case ("n_values", JInt(v)) => Some(v.toInt)
            case ("kind", JString(v)) => Some(v)
            case ("keyed", JBool(v)) => Some(v)
            case _ => None
          }
        } catch {
          case e: NumberFormatException =>
            None
        }

        if (value.isDefined) {
          val definition = result.getOrElse(name, MMap[String, Option[Any]]())
          result(name) = definition
          definition(k) = value
        }
      }

      val pretty = for {
        (k, v) <- result
      } yield {
        (k, HistogramDefinition(v("kind").get.asInstanceOf[String],
                                v.getOrElse("keyed", Some(false)).get.asInstanceOf[Boolean],
                                v.getOrElse("n_values", None).asInstanceOf[Option[Int]],
                                v.getOrElse("low", None).asInstanceOf[Option[Int]],
                                v.getOrElse("high", None).asInstanceOf[Option[Int]],
                                v.getOrElse("n_buckets", None).asInstanceOf[Option[Int]]))
      }

      (key, pretty)
    }

    // Histograms are considered to be immutable so it's OK to merge their definitions
    parsed.flatMap(_._2).toMap
  }
}
