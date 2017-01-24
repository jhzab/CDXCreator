name := "CDXExtractor"

version := "0.1"

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding",
  "UTF-8", // yes, this is 2 args
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code", // N.B. doesn't work well with the ??? hole
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture",
  "-optimize",
  "-Ywarn-unused-import" // 2.11 only
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.netpreserve.commons" % "webarchive-commons" % "1.1.6" exclude ("org.apache.hadoop", "hadoop-core"),
  "com.github.scopt" %% "scopt" % "3.5.0",
  "org.typelevel" %% "cats" % "0.9.0",
  "commons-io" % "commons-io" % "2.5",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

wartremoverWarnings in (Compile, compile) ++= Warts
  .allBut(Wart.Var, Wart.Null, Wart.AsInstanceOf, Wart.MutableDataStructures)
