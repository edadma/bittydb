# BittyDB

BittyDB is a small embeddable database engine for the JVM for storing documents (i.e. JSON-like objects).  The implementation is in Scala, however, this project will aim to be very Java friendly as well.

This project is under active development and a release has not yet been made.


Example
-------

Here is a simple example demonstrating database insertion, querying and deletion.

	import ca.hyperreal.bittydb

	object Main extends App
	{
	  val db = Connection.mem()
	
	  // insert some documents into a collection named 'test'
	  db.collection( "test" ).insert( Map("a" -> 1, "b" -> "first"), Map("a" -> 1, "b" -> "second"), Map("a" -> 2, "b" -> "third") )
	  println( db.root("test").get )
	
	  // query collection 'test' for a document with property 'a' equal to 1
	  println( db.collection( "test" ) find (_("a") === 1) toList )
	
	  // remove from collection 'test' any document with property 'a' equal to 1
	  db.collection( "test" ) remove (_("a") === 1)
	  println( db.root("test").get )
	}

The above example should output something very similar (the UUID's will be different) to

	List(Map(a -> 1, b -> first, _id -> 0c3c9dd7-5205-41ab-8d3c-4b017bd563d3), Map(a -> 1, b -> second, _id -> ec905af2-11e0-41db-8e1a-b5f450343c75), Map(a -> 2, b -> third, _id -> 7d53d16d-b869-4041-91db-e58d409e6c43))
	List(Map(a -> 1, b -> first, _id -> 0c3c9dd7-5205-41ab-8d3c-4b017bd563d3), Map(a -> 1, b -> second, _id -> ec905af2-11e0-41db-8e1a-b5f450343c75))
	List(Map(a -> 2, b -> third, _id -> 7d53d16d-b869-4041-91db-e58d409e6c43))
	
	
## License

BittyDB is distributed under the MIT License, meaning that you are free to use it in your free or proprietary software.


## Building

### Requirements

- SBT 13.8+
- Java 8+

### Clone and build

	git clone git://github.com/edadma/bittydb.git
	cd bittydb
	sbt publishLocal
	
add the following to your build.sbt
	
	libraryDependencies += "ca.hyperreal" %% "bittydb" % "0.1"
