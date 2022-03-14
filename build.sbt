ThisBuild / licenses += "ISC" -> url("https://opensource.org/licenses/ISC")
ThisBuild / versionScheme := Some("semver-spec")

lazy val bittydb = crossProject( /*JSPlatform,*/ JVMPlatform /*, NativePlatform*/ )
  .in(file("."))
  .settings(
    name := "bittydb",
    version := "0.1.0",
    scalaVersion := "3.1.1",
    scalacOptions ++=
      Seq(
        "-deprecation",
        "-feature",
        "-unchecked",
        "-language:postfixOps",
        "-language:implicitConversions",
        "-language:existentials",
        "-language:dynamics",
      ),
    organization := "io.github.edadma",
    githubOwner := "edadma",
    githubRepository := name.value,
    libraryDependencies += "org.scalatest" %%% "scalatest" % "3.2.11" % "test",
    libraryDependencies ++= Seq(
      "io.github.edadma" %%% "dal" % "0.1.9",
      "com.lihaoyi" %%% "pprint" % "0.7.1",
    ),
    publishMavenStyle := true,
    Test / publishArtifact := false,
  )
  .jvmSettings(
    libraryDependencies += "org.scala-js" %% "scalajs-stubs" % "1.1.0" % "provided",
  )
//  .nativeSettings(
//    nativeLinkStubs := true
//  )
//  .jsSettings(
//    jsEnv := new org.scalajs.jsenv.nodejs.NodeJSEnv(),
//    //    Test / scalaJSUseMainModuleInitializer := true,
//    //    Test / scalaJSUseTestModuleInitializer := false,
//    Test / scalaJSUseMainModuleInitializer := false,
//    Test / scalaJSUseTestModuleInitializer := true,
//    scalaJSUseMainModuleInitializer := true,
//  )
