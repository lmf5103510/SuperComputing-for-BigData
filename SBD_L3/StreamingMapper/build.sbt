name := "StreamingMapper"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-streaming_2.11" % "2.0.0"
)

resourceDirectory in Compile := baseDirectory.value / "resources"

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
