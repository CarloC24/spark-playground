ThisBuild / organization := "playground"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.13.18"

val sparkVersion = "4.2.0"

// Spark reaches deep into JDK internals, so it needs these opened up on JDK 17+.
val sparkJavaOptions = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false",
  "-Dio.netty.tryReflectionSetAccessible=true"
)

lazy val root = (project in file("."))
  .settings(
    name := "spark-playground",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql"  % sparkVersion
    ),
    // Maven owns src/main/java (see pom.xml); sbt compiles only Scala. Without this,
    // sbt also picks up the Java sources and `sbt run` becomes ambiguous because it
    // finds two main classes.
    Compile / unmanagedSourceDirectories := Seq((Compile / scalaSource).value),

    // Spark must run in its own JVM so the --add-opens flags above take effect.
    run / fork    := true,
    Test / fork   := true,
    javaOptions ++= sparkJavaOptions
  )
