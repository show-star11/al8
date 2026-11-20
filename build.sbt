name := "GraphXPageRank"

version := "1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.7",
  "org.apache.spark" %% "spark-sql" % "3.5.7",
  "org.apache.spark" %% "spark-graphx" % "3.5.7"
)

// "sbt run" runs Example 1; Example 2 runs with: sbt "runMain CitationPageRank"
Compile / run / mainClass := Some("GraphXPageRank")

// Run the program in its own Java process, so Spark starts and stops cleanly
fork := true

// Print the program's output as it is, without [info] in front of every line
outputStrategy := Some(StdoutOutput)

// Java 17 blocks some internal access that Spark needs; these options allow it
javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)
