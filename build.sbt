name := "study"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

assemblyMergeStrategy in assembly := {
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case _ => MergeStrategy.first
}
