import com.typesafe.sbt.packager.SettingsHelper.makeDeploymentSettings
import aether.AetherKeys._
import NativePackagerHelper._

lazy val artifactSettings = Seq(
  organization := "com.smps",
  name := "datalake-consumer-event"
)

lazy val archrefSpark = (project in file(".")).
  settings(artifactSettings: _*).
  settings(
    scalaVersion := "2.11.12",
    // disable using the Scala version in output paths and artifacts
    crossPaths := false,
    parallelExecution in Test := false
  )

lazy val credentialsPath = Path.userHome / ".sbt" / ".credentials"
credentials += Credentials(credentialsPath)

lazy val sparkVersion = "2.4.3"
lazy val hadoopVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion excludeAll ExclusionRule(organization = "com.sun.jersey"),
  "org.apache.hadoop" % "hadoop-azure" % hadoopVersion excludeAll ExclusionRule(organization = "com.sun.jersey"),
  "org.apache.kafka" %% "kafka" % "2.3.0",
  "com.google.code.gson" % "gson" % "2.8.5",
  "org.kitesdk" % "kite-data-core" % "1.1.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.mockito" % "mockito-all" % "1.9.5" % Test
)

// override default publish and publishLocal task
overridePublishBothSettings
mappings in Universal ++= directory("config/")
publishTo := Some("default" at "https://pkgs.dev.azure.com/albatross-getnet/_packaging/Analytics/maven/v1")

lazy val packageBinExtension =  "tgz"

makeDeploymentSettings(Universal, packageBin in Universal, packageBinExtension)
publishLocal := publishLocal.dependsOn(publishLocal in Universal).value
enablePlugins(JavaAppPackaging, UniversalDeployPlugin, AetherPlugin)

aetherArtifact := {
  val artifact = aetherArtifact.value
  artifact.attach((packageBin in Universal).value, "dist", packageBinExtension)
}
