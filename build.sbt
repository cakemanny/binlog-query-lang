
organization := "com.cakemanny"

name := "binlog-query"

version := "1.0-SNAPSHOT"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.3.2",
  "org.scalaz" %% "scalaz-core" % "7.2.14",
  "com.slamdata" %% "matryoshka-core" % "0.18.3",
  "com.lihaoyi" %% "fastparse" % "0.4.2",
  //"com.github.pathikrit"  %% "better-files-akka"  % "3.0.0",
  "com.github.shyiko" % "mysql-binlog-connector-java" % "0.13.0",
  "com.lihaoyi" % "ammonite" % "1.0.1" % "test" cross CrossVersion.full
)

scalacOptions += "-Ypartial-unification"

sourceGenerators in Test += Def.task {
  val file = (sourceManaged in Test).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main().run() }""")
  Seq(file)
}.taskValue

