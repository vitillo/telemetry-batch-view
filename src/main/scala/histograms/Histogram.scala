package telemetry.histograms

import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.io.Source

case class RawHistogram(values: Map[String, Long], sum: Long)

case class HistogramDefinition(alert_emails: List[String],
                     expires_in_version: String,
                     kind: String,
                     n_values: Option[Int],
                     keyed: Option[Boolean],
                     description: String) {
}

object Histograms {
  val definitions = {
    implicit val formats = DefaultFormats

    val uris = Map("release" -> "https://hg.mozilla.org/releases/mozilla-release/raw-file/tip/toolkit/components/telemetry/Histograms.json",
                   "beta" -> "https://hg.mozilla.org/releases/mozilla-beta/raw-file/tip/toolkit/components/telemetry/Histograms.json",
                   "aurora" -> "https://hg.mozilla.org/releases/mozilla-aurora/raw-file/tip/toolkit/components/telemetry/Histograms.json",
                   "nightly" -> "https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/Histograms.json")

    val parsed = uris.map{ case (key, value) =>
      (key, parse(Source.fromURL(value, "UTF8").mkString).extract[Map[String, HistogramDefinition]])
    }

    // Histograms are considered to be immutable so it's OK to merge their definitions
    parsed.flatMap(_._2).toMap
  }
}
