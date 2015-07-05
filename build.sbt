import AssemblyKeys._


name := "bittydb"

version := "0.1"

scalaVersion := "2.11.6"

scalacOptions ++= Seq( "-deprecation", "-feature", "-language:postfixOps", "-language:implicitConversions", "-language:existentials" )

incOptions := incOptions.value.withNameHashing( true )

organization := "ca.hyperreal"

//resolvers += Resolver.sonatypeRepo( "snapshots" )

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Hyperreal Repository" at "https://dl.bintray.com/edadma/maven"

libraryDependencies ++= Seq(
	"org.scalatest" %% "scalatest" % "2.2.1" % "test",
	"org.scalacheck" %% "scalacheck" % "1.11.5" % "test"
	)

//libraryDependencies ++= Seq(
//	"org.slf4j" % "slf4j-api" % "1.7.7",
//	"org.slf4j" % "slf4j-simple" % "1.7.7"
//	)

mainClass in (Compile, run) := Some( "ca.hyperreal." + name.value + ".Main" )

assemblySettings

mainClass in assembly := Some( "ca.hyperreal." + name.value + ".Main" )

jarName in assembly := name.value + "-" + version.value + ".jar"

seq(bintraySettings:_*)

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/edadma/sscheme"))

pomExtra := (
  <scm>
    <url>git@github.com:edadma/sscheme.git</url>
    <connection>scm:git:git@github.com:edadma/sscheme.git</connection>
  </scm>
  <developers>
    <developer>
      <id>edadma</id>
      <name>Edward A. Maxedon, Sr.</name>
      <url>http://hyperreal.ca</url>
    </developer>
  </developers>)
