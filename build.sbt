lazy val root = (project in file(".")).
  settings(
    name := "tachyon-fuse",

    version := "0.1-SNAPSHOT",

    resolvers ++= Seq(
      "Local Maven Repository" at String.format("file://%s/.m2/repository", Path.userHome.absolutePath),
      "Bintray central" at "http://jcenter.bintray.com"
    ),

    libraryDependencies ++= Seq(
      "com.github.serceman" % "jnr-fuse" % "0.1",
      "org.slf4j" % "slf4j-api" % "1.7.13",
      "com.google.guava" % "guava" % "18.0",
      "commons-cli" % "commons-cli" % "1.3.1",
      "org.tachyonproject" % "tachyon-client" % "0.8.2" % "provided"
      ),

    mainClass in (Compile, packageBin) := Some("com.ibm.ie.tachyon.fuse.TachyonFuse"),
    mainClass in (Compile, run) := Some("com.ibm.ie.tachyon.fuse.TachyonFuse"),

    fork in run := true,

    javacOptions  ++= Seq("-source", "1.8", "-target", "1.8"),

    javaOptions in run += "-Dlog4j.configuration=log4j.properties",

    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case _ => MergeStrategy.first
    },

    assemblyOutputPath in assembly := file(s"target/${name.value}-assembly-${version.value}.jar")
  )

