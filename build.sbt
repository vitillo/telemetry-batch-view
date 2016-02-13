javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint", "-Xss2M")

lazy val root = (project in file(".")).
  settings(
    name := "telemetry-batch-view",
    version := "1.1",
    scalaVersion := "2.10.6",
    retrieveManaged := true,
    ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },
    libraryDependencies += "org.apache.avro" % "avro" % "1.7.7",
    libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.8.1",
    libraryDependencies += "com.github.seratch" %% "awscala" % "0.3.+",
    libraryDependencies += "net.sandrogrzicic" %% "scalabuff-runtime" % "1.4.0",
    libraryDependencies += "com.typesafe" % "config" % "1.2.1",
    libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test",
    libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.2",
    libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.11",
    libraryDependencies += "joda-time" % "joda-time" % "2.9.1",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0" excludeAll(ExclusionRule(organization = "javax.servlet")),
    libraryDependencies += "org.apache.spark" %% "spark-yarn" % "1.5.0"
  )

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
assemblyJarName in assembly := s"telemetry-batch-view-${version.value}.jar"
test in assembly := {}

mergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case "reference.conf"              => MergeStrategy.concat
  case x => MergeStrategy.first
}
