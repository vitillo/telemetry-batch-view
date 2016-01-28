package telemetry.histograms

import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.{Map => MMap}
import scala.io.Source

case class RawHistogram(values: Map[String, Long], sum: Long)

sealed abstract class HistogramDefinition
case class FlagHistogram(keyed: Boolean) extends HistogramDefinition
case class BooleanHistogram(keyed: Boolean) extends HistogramDefinition
case class CountHistogram(keyed: Boolean) extends HistogramDefinition
case class EnumeratedHistogram(keyed: Boolean, nValues: Int) extends HistogramDefinition
case class LinearHistogram(keyed: Boolean, low: Int, high: Int, nBuckets: Int) extends HistogramDefinition
case class ExponentialHistogram(keyed: Boolean, low: Int, high: Int, nBuckets: Int) extends HistogramDefinition

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
            case ("keyed", JString(v)) => v match {
              case "true" => Some(true)
              case _ => Some(false)
            }
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
        val kind = v("kind").get.asInstanceOf[String]
        val keyed = v.getOrElse("keyed", Some(false)).get.asInstanceOf[Boolean]
        val nValues = v.getOrElse("n_values", None).asInstanceOf[Option[Int]]
        val low = v.getOrElse("low", Some(1)).get.asInstanceOf[Int]
        val high = v.getOrElse("high", None).asInstanceOf[Option[Int]]
        val nBuckets = v.getOrElse("n_buckets", None).asInstanceOf[Option[Int]]

        (kind, nValues, high, nBuckets) match {
          case ("flag", _, _, _) =>
            Some((k, FlagHistogram(keyed)))
          case ("boolean", _, _ , _) =>
            Some((k, BooleanHistogram(keyed)))
          case ("count", _, _, _) =>
            Some((k, CountHistogram(keyed)))
          case ("enumerated", Some(nValues), _, _) =>
            Some((k, EnumeratedHistogram(keyed, nValues)))
          case ("linear", _, Some(high), Some(nBuckets)) =>
            Some((k, LinearHistogram(keyed, low, high, nBuckets)))
          case ("exponential", _, Some(high), Some(nBuckets)) =>
            Some((k, ExponentialHistogram(keyed, low, high, nBuckets)))
          case _ =>
            None
        }
      }

      (key, pretty.flatten.toMap)
    }

    // Histograms are considered to be immutable so it's OK to merge their definitions
    parsed.flatMap(_._2).toMap
  }

  def linearBuckets(min: Float, max: Float, nBuckets: Int): Array[Int] = {
    lazy val buckets = {
      var values = Array.fill(nBuckets){0}

      for(i <- 1 until nBuckets) {
        val linearRange = (min * (nBuckets - 1 - i) + max * (i - 1)) / (nBuckets - 2)
        values(i) = (linearRange + 0.5).toInt
      }

      values
    }

    memoLinearBuckets.getOrElseUpdate((min, max, nBuckets), buckets)
  }

  def exponentialBuckets(min: Float, max: Float, nBuckets: Int): Array[Int] = {
    lazy val buckets = {
      val logMax = math.log(max)
      val bucketIndex = 2
      val retArray = Array.fill(nBuckets){0}
      var current = min.toInt

      retArray(1) = current.toInt
      for (bucketIndex <- 2 until nBuckets) {
        val logCurrent = math.log(current)
        val logRatio = (logMax - logCurrent) / (nBuckets - bucketIndex)
        val logNext = logCurrent + logRatio
        val nextValue = math.floor(math.exp(logNext) + 0.5).toInt

        if (nextValue > current)
          current = nextValue
        else
          current = current + 1
        retArray(bucketIndex) = current
      }

      retArray
    }

    memoExponentialBuckets.getOrElseUpdate((min, max, nBuckets), buckets)
  }

  private val memoLinearBuckets = MMap[Tuple3[Float, Float, Int], Array[Int]]()
  private val memoExponentialBuckets = MMap[Tuple3[Float, Float, Int], Array[Int]]()
}
