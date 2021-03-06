package com.mozilla.telemetry

import com.mozilla.telemetry.heka.HekaFrame
import com.mozilla.telemetry.utils.MainPing
import com.mozilla.telemetry.views.MainSummaryView
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}

class MainSummaryViewTest extends FlatSpec with Matchers{
  val testPayload = """
{
 "environment": {
  "addons": {
   "activeAddons": {
    "addon 1": {
      "blocklisted": false,
      "description": "First example addon.",
      "name": "Example 1",
      "userDisabled": false,
      "appDisabled": false,
      "version": "1.0",
      "scope": 1,
      "type": "extension",
      "foreignInstall": false,
      "hasBinaryComponents": false,
      "installDay": 16861,
      "updateDay": 16875,
      "isSystem": true
    },
    "addon 2": {
      "blocklisted": false,
      "description": "Second example addon.",
      "name": "Example 2",
      "userDisabled": false,
      "appDisabled": false,
      "version": "1.0",
      "scope": 1,
      "type": "extension",
      "foreignInstall": false,
      "hasBinaryComponents": false,
      "installDay": 16862,
      "updateDay": 16880,
      "isSystem": false
    },
    "addon 3": {
      "blocklisted": false,
      "description": "Third example addon.",
      "name": "Example 3",
      "userDisabled": false,
      "appDisabled": false,
      "version": "1.0",
      "scope": 1,
      "type": "extension",
      "foreignInstall": false,
      "hasBinaryComponents": false,
      "installDay": 16865,
      "updateDay": 16890,
      "isSystem": false
    }
   },
   "activePlugins": [
    {
     "name": "Default Browser Helper",
     "version": "601",
     "description": "Provides information about the default web browser",
     "blocklisted": false,
     "disabled": false,
     "clicktoplay": true,
     "mimeTypes": ["application/apple-default-browser"],
     "updateDay": 16780
    },
    {
     "name": "Java Applet Plug-in",
     "version": "Java 8 Update 73 build 02",
     "description": "Displays Java applet content, or a placeholder if Java is not installed.",
     "blocklisted": false,
     "disabled": false,
     "clicktoplay": true,
     "mimeTypes": [
      "application/x-java-applet;jpi-version=1.8.0_73",
      "application/x-java-applet;version=1.5"
     ],
     "updateDay": 16829
    },
    {
     "name": "Shockwave Flash",
     "description": "Example Flash 1",
     "version": "19.0.0.226"
    },
    {
     "name": "Shockwave Flash",
     "description": "Example Flash 2",
     "version": "19.0.0.225"
    },
    {
     "name": "Shockwave Flash",
     "description": "Example Flash 3",
     "version": "9.9.9.227"
    }
   ]
  }
 },
 "payload": {
  "emptyKey": {},
  "keyedHistograms": {
   "SEARCH_COUNTS": {
    "test.urlbar": {
     "range": [1, 2],
     "bucket_count": 3,
     "histogram_type": 4,
     "values": {"0": 78, "1": 0},
     "sum": 78,
     "sum_squares_lo": 78,
     "sum_squares_hi": 0
    },
    "test.abouthome": {
     "range": [1, 2],
     "bucket_count": 3,
     "histogram_type": 4,
     "values": {"0": 10, "1": 0},
     "sum": 10,
     "sum_squares_lo": 10,
     "sum_squares_hi": 0
    }
   }
  }
 }
}
"""

  "A json object's keys" can "be counted" in {
    val json = parse(testPayload)

    val countKeys = MainPing.countKeys _
    countKeys(json \ "environment" \ "addons" \ "activeAddons").get should be (3)
    countKeys(json).get should be (2)
    countKeys(json \ "payload").get should be (2)
    countKeys(json \ "payload" \ "emptyKey").get should be (0)
    countKeys(json \ "dummy") should be (None)
  }

  "Latest flash version" can "be extracted" in {
    // Valid data
    val json = parse(testPayload)
    val getFlash = MainPing.getFlashVersion _
    getFlash(json \ "environment" \ "addons").get should be ("19.0.0.226")
    getFlash(json \ "environment") should be (None)
    getFlash(json \ "foo") should be (None)

    // Contains plugins, but not Flash:
    val json2 = parse(
      """
        |{
        | "environment": {
        |  "addons": {
        |   "activePlugins": [
        |    {
        |     "name": "Default Browser Helper",
        |     "version": "601",
        |     "description": "Provides information about the default web browser"
        |    },
        |    {
        |     "name": "Java Applet Plug-in",
        |     "version": "Java 8 Update 73 build 02",
        |     "description": "Displays Java applet content"
        |    }
        |   ]
        |  }
        | }
        |}
      """.stripMargin)
    getFlash(json2 \ "environment" \ "addons") should be (None)

    // Doesn't contain any plugins:
    val json3 = parse(
      """
        |{
        | "environment": {
        |  "addons": {
        |   "activePlugins": []
        |  }
        | }
        |}
      """.stripMargin)
    getFlash(json3 \ "environment" \ "addons") should be (None)

    // Contains many plugins, some with invalid versions
    val json4 = parse(
      """
        |{
        | "environment": {
        |  "addons": {
        |   "activePlugins": [
        |    {
        |     "name": "Shockwave Flash",
        |     "description": "Example Flash 1",
        |     "version": "19.0.0.g226"
        |    },
        |    {
        |     "name": "Shockwave Flash",
        |     "description": "Example Flash 2",
        |     "version": "19.0.0.225"
        |    },
        |    {
        |     "name": "Shockwave Flash",
        |     "description": "Example Flash 3",
        |     "version": "9.9.9.227"
        |    },
        |    {
        |     "name": "Shockwave Flash",
        |     "description": "Example Flash 4",
        |     "version": "999.x.y.227"
        |    }
        |   ]
        |  }
        | },
        | "payload": {
        |  "emptyKey": {}
        | }
        |}
      """.stripMargin)
    getFlash(json4 \ "environment" \ "addons").get should be ("19.0.0.225")
  }

  "Flash versions" can "be compared" in {
    val cmpFlash = MainPing.compareFlashVersions _
    cmpFlash(Some("1.2.3.4"), Some("1.2.3.4")).get should be (0)
    cmpFlash(Some("1.2.3.5"), Some("1.2.3.4")).get should be (1)
    cmpFlash(Some("1.2.3.4"), Some("1.2.3.5")).get should be (-1)

    // Lexically less, but numerically greater:
    cmpFlash(Some("10.2.3.5"), Some("9.3.4.8")).get should be (1)
    cmpFlash(Some("foo"), Some("1.2.3.4")).get should be (-1)
    cmpFlash(Some("1.2.3.4"), Some("foo")).get should be (1)
    cmpFlash(Some("foo"), Some("bar")) should be (None)

    // Equal but bogus values are equal (for efficiency).
    cmpFlash(Some("foo"), Some("foo")).get should be (0)

    // Something > Nothing
    cmpFlash(Some("1.2.3.5"), None).get should be (1)
    cmpFlash(None, Some("1.2.3.5")).get should be (-1)
  }
  val exampleSearches = parse("""
      |{
      |  "google.abouthome": {
      |    "range": [1, 2],
      |    "bucket_count": 3,
      |    "histogram_type": 4,
      |    "values": {"0": 1, "1": 0},
      |    "sum": 1,
      |    "sum_squares_lo": 1,
      |    "sum_squares_hi": 0
      |  },
      |  "google.urlbar": {
      |    "range": [1, 2],
      |    "bucket_count": 3,
      |    "histogram_type": 4,
      |    "values": {"0": 67, "1": 0},
      |    "sum": 67,
      |    "sum_squares_lo": 67,
      |    "sum_squares_hi": 0
      |  },
      |  "yahoo.urlbar": {
      |    "range": [1, 2],
      |    "bucket_count": 3,
      |    "histogram_type": 4,
      |    "values": {"0": 78, "1": 0},
      |    "sum": 78,
      |    "sum_squares_lo": 78,
      |    "sum_squares_hi": 0
      |  },
      |  "toast1": {
      |    "range": [1, 2],
      |    "bucket_count": 3,
      |    "histogram_type": 4,
      |    "values": {"0": 100, "1": 0},
      |    "sum": "toast",
      |    "sum_squares_lo": 100,
      |    "sum_squares_hi": 0
      |  },
      |  "toast2": {
      |    "range": [1, 2],
      |    "bucket_count": 3,
      |    "histogram_type": 4,
      |    "values": {"0": 10, "1": 0},
      |    "sum": 10,
      |    "sum_squares_lo": 10,
      |    "sum_squares_hi": 0
      |  },
      |  "toast3.badcount": {
      |    "range": [1, 2],
      |    "bucket_count": 3,
      |    "histogram_type": 4,
      |    "values": {"0": 100, "1": 0},
      |    "sum": "toast",
      |    "sum_squares_lo": 100,
      |    "sum_squares_hi": 0
      |  }
      |}
    """.stripMargin)

  "Search counts" can "be converted" in {
    var expected = 0l
    for ((k, e, s, c) <- List(
      ("google.abouthome", "google", "abouthome", 1l),
      ("google.urlbar",    "google", "urlbar",    67l),
      ("yahoo.urlbar",     "yahoo",  "urlbar",    78l),
      ("toast1",           null,     null,        null),
      ("toast2",           null,     null,        10l),
      ("toast3.badcount",  "toast3", "badcount",  null))) {
      val m = MainPing.searchHistogramToRow(k, exampleSearches \ k)
      m(0) shouldBe e
      m(1) shouldBe s
      m(2) shouldBe c
      expected = expected + (c match {
        case x: Long => x
        case _ => 0
      })
    }

    MainPing.searchHistogramToRow("toast1", exampleSearches \ "toast1") should be (Row(null, null, null))

    var actual = 0l
    for (search <- MainPing.getSearchCounts(exampleSearches).get) {

      actual = actual + (search.get(2) match {
        case x: Long => x
        case _ => 0
      })
    }
    actual should be (expected)

    val json = parse(testPayload)
    var payloadCount = 0l
    for (search <- MainPing.getSearchCounts(json \ "payload" \ "keyedHistograms" \ "SEARCH_COUNTS").get) {
      payloadCount = payloadCount + search.getLong(2)
    }
    payloadCount should be (88l)
  }

  "Histogram means" can "be computed" in {
    val example = parse("""{
      |  "H1": {
      |    "bucket_count": 20,
      |    "histogram_type": 0,
      |    "log_sum": 0,
      |    "log_sum_squares": 0,
      |    "range": [1000, 150000],
      |    "sum": 30798,
      |    "values": {
      |      "21371": 0,
      |      "28231": 1,
      |      "37292": 0
      |    }
      |  },
      |  "H2": {
      |    "bucket_count": 20,
      |    "histogram_type": 0,
      |    "log_sum": 0,
      |    "log_sum_squares": 0,
      |    "range": [1000, 150000],
      |    "sum": 30798,
      |    "values": {
      |      "21371": 0,
      |      "28231": 2,
      |      "37292": 0
      |    }
      |  },
      |  "H3": {
      |    "bucket_count": 20,
      |    "histogram_type": 0,
      |    "log_sum": 0,
      |    "log_sum_squares": 0,
      |    "range": [1000, 150000],
      |    "sum": 30798,
      |    "values": {
      |      "21371": 1,
      |      "28231": 2,
      |      "37292": 0
      |    }
      |  },
      |  "H4": {
      |    "bucket_count": 20,
      |    "histogram_type": 0,
      |    "log_sum": 0,
      |    "log_sum_squares": 0,
      |    "range": [1000, 150000],
      |    "sum": 30798,
      |    "values": {
      |      "21371": 1,
      |      "28231": 2,
      |      "37292": 1
      |    }
      |  },
      |  "H5": {
      |    "bucket_count": 20,
      |    "histogram_type": 0,
      |    "log_sum": 0,
      |    "log_sum_squares": 0,
      |    "range": [1000, 150000],
      |    "sum": 0,
      |    "values": {
      |      "21371": 1,
      |      "28231": 2,
      |      "37292": 1
      |    }
      |  },
      |  "H6": {
      |    "bucket_count": 20,
      |    "histogram_type": 0,
      |    "log_sum": 0,
      |    "log_sum_squares": 0,
      |    "range": [1000, 150000],
      |    "sum": 10,
      |    "values": {
      |      "21371": 0,
      |      "28231": 0,
      |      "37292": 0
      |    }
      |  }
      |}""".stripMargin)
    MainPing.histogramToMean(example \ "H1").get should be (30798)
    MainPing.histogramToMean(example \ "H2").get should be (15399)
    MainPing.histogramToMean(example \ "H3").get should be (10266)
    MainPing.histogramToMean(example \ "H4").get should be (7699)
    MainPing.histogramToMean(example \ "H5").get should be (0) // Sum is zero
    MainPing.histogramToMean(example \ "H6") should be (None) // bucket counts sum to zero
    MainPing.histogramToMean(example \ "H0") should be (None) // Missing
  }

  "Enum Histograms" can "be converted to Rows" in {
    val example = parse("""{
      |  "H1": {
      |    "bucket_count": 5,
      |    "histogram_type": 1,
      |    "sum": 30798,
      |    "values": {
      |      "0": 1,
      |      "1": 2,
      |      "3": 4
      |    }
      |  }
      |}""".stripMargin)
    MainPing.enumHistogramToRow(example \ "H1", (0 to 3).map(_.toString)) should be (Row(1, 2, 0, 4))
  }

  "Keyed Enum Histograms" can "be converted to Maps of Rows" in {
    val example = parse("""{
      |  "H1": {
      |    "foo": {
      |      "bucket_count": 5,
      |      "histogram_type": 1,
      |      "sum": 30798,
      |      "values": {
      |        "0": 1,
      |        "2": 2
      |      }
      |    },
      |    "bar": {
      |      "bucket_count": 5,
      |      "histogram_type": 1,
      |      "sum": 30798,
      |      "values": {
      |        "0": 5,
      |        "1": 1,
      |        "2": 3
      |      }
      |    }
      |  }
      |}""".stripMargin)
    val expected = Map[String,Row](
      "foo" -> Row(1,0,2),
      "bar" -> Row(5,1,3)
    )
    MainPing.keyedEnumHistogramToMap(example \ "H1", (0 to 2).map(_.toString)).get should be (expected)
  }

  "MainSummary records" can "be serialized" in {
    val sparkConf = new SparkConf().setAppName("MainSummary")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    try {
      val schema = MainSummaryView.buildSchema

      // Use an example framed-heka message. It is based on test_main.json.gz,
      // submitted with a URL of
      //    /submit/telemetry/foo/main/Firefox/48.0a1/nightly/20160315030230
      val hekaFileName = "/test_main.snappy.heka"
      val hekaURL = getClass.getResource(hekaFileName)
      val input = hekaURL.openStream()
      val rows = HekaFrame.parse(input).flatMap(MainSummaryView.messageToRow)

      // Serialize this one row as Parquet
      val sqlContext = new SQLContext(sc)
      val dataframe = sqlContext.createDataFrame(sc.parallelize(rows.toSeq), schema)
      val tempFile = com.mozilla.telemetry.utils.temporaryFileName()
      dataframe.write.parquet(tempFile.toString)

      // Then read it back
      val data = sqlContext.read.parquet(tempFile.toString)

      data.count() should be (1)
      data.filter(data("document_id") === "foo").count() should be (1)
    } finally {
      sc.stop()
    }
  }

  // Apply the given schema to the given potentially-generic Row.
  def applySchema(row: Row, schema: StructType): Row = new GenericRowWithSchema(row.toSeq.toArray, schema)

  "Heka records" can "be summarized" in {
    // Use an example framed-heka message. It is based on test_main.json.gz,
    // submitted with a URL of
    //    /submit/telemetry/foo/main/Firefox/48.0a1/nightly/20160315030230
    val hekaFileName = "/test_main.snappy.heka"
    val hekaURL = getClass.getResource(hekaFileName)
    val input = hekaURL.openStream()

    val schema = MainSummaryView.buildSchema
    val searchField = schema.fields.filter(p => p.name == "search_counts").head
    val searchSchema = searchField.dataType match {
      case searchCountsType: ArrayType =>
        searchCountsType.elementType match {
          case searchCountsStruct: StructType => searchCountsStruct
        }
    }

    var count = 0
    for (message <- HekaFrame.parse(input)) {
      message.timestamp should be (1460036116829920000l)
      message.`type`.get should be ("telemetry")
      message.logger.get should be ("telemetry")

      for (summary <- MainSummaryView.messageToRow(message)) {
        // Apply our schema to a generic Row object
        val r = applySchema(summary, schema)

        val expected = Map(
          "document_id"                     -> "foo",
          "client_id"                       -> "c4582ba1-79fc-1f47-ae2a-671118dccd8b",
          "sample_id"                       -> 4l,
          "channel"                         -> "nightly",
          "normalized_channel"              -> "nightly",
          "country"                         -> "??",
          "city"                            -> "??",
          "os"                              -> "Darwin",
          "os_version"                      -> "15.3.0",
          "os_service_pack_major"           -> null,
          "os_service_pack_minor"           -> null,
          "profile_creation_date"           -> 16861l,
          "subsession_start_date"           -> "2016-03-28T00:00:00.0-03:00",
          "subsession_length"               -> 14557l,
          "distribution_id"                 -> null,
          "submission_date"                 -> "20160407",
          "sync_configured"                 -> false,
          "sync_count_desktop"              -> null,
          "sync_count_mobile"               -> null,
          "app_build_id"                    -> "20160315030230",
          "app_display_version"             -> "48.0a1",
          "app_name"                        -> "Firefox",
          "app_version"                     -> "48.0a1",
          "timestamp"                       -> 1460036116829920000l,
          "env_build_id"                    -> "20160315030230",
          "env_build_version"               -> "48.0a1",
          "env_build_arch"                  -> "x86-64",
          "e10s_enabled"                    -> true,
          "e10s_cohort"                     -> "unsupportedChannel",
          "locale"                          -> "en-US",
          "active_experiment_id"            -> null,
          "active_experiment_branch"        -> null,
          "reason"                          -> "gather-payload",
          "timezone_offset"                 -> -180,
          "plugin_hangs"                    -> null,
          "aborts_plugin"                   -> null,
          "aborts_content"                  -> null,
          "aborts_gmplugin"                 -> null,
          "crashes_detected_plugin"         -> null,
          "crashes_detected_content"        -> null,
          "crashes_detected_gmplugin"       -> null,
          "crash_submit_attempt_main"       -> null,
          "crash_submit_attempt_content"    -> null,
          "crash_submit_attempt_plugin"     -> null,
          "crash_submit_success_main"       -> null,
          "crash_submit_success_content"    -> null,
          "crash_submit_success_plugin"     -> null,
          "active_addons_count"             -> 3l,
          "flash_version"                   -> null,
          "vendor"                          -> "Mozilla",
          "is_default_browser"              -> true,
          "default_search_engine_data_name" -> "Google",
          "default_search_engine"           -> "google",
          "devtools_toolbox_opened_count"   -> 3,
          "client_submission_date"          -> null,
          "push_api_notification_received"  -> null,
          "web_notification_shown"          -> null,
          "places_pages_count"              -> 104849,
          "places_bookmarks_count"          -> 183
        )

        val actual = r.getValuesMap(expected.keys.toList)
        for ((f, v) <- expected) {
          actual.get(f) should be (Some(v))
        }
        actual should be (expected)

        val searches = r.getSeq[Row](r.fieldIndex("search_counts"))
        val searchCounter = searches.map(search => {
          val sW = applySchema(search, searchSchema)
          sW.getLong(sW.fieldIndex("count"))
        }).sum
        searchCounter should be (65l)

        r.getStruct(r.fieldIndex("loop_activity_counter")) should be (null)
        val popup = r.getMap[String,Row](r.fieldIndex("popup_notification_stats"))
        val expectedPopup = Map[String,Row](
          "(all)"             -> Row(8,2,0,0,0,1,0,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0),
          "geolocation"       -> Row(1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0),
          "password"          -> Row(5,0,0,0,0,1,0,4,0,0,0,0,0,0,0,0,0,0,0,0,0,0),
          "web-notifications" -> Row(2,1,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0))
        popup should be (expectedPopup)

        count += 1
      }
    }
    input.close()
    count should be (1)
  }

  "Job parameters" can "conform to expected values" in {
    MainSummaryView.jobName should be ("main_summary")
    val versionPattern = "^v[0-9]+$".r
    (versionPattern findAllIn MainSummaryView.streamVersion).mkString("Oops") should be (MainSummaryView.streamVersion)
  }
}
