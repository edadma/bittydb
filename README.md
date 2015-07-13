# BittyDB

BittyDB is a small embeddable database engine for the JVM for storing documents (i.e. JSON-like objects).  The implementation is in Scala, however, this project will aim to be very Java friendly as well.

This project is under active development and a release has not yet been made.


Example
-------

Here is a simple example demonstrating database insertion, querying and deletion.

	import ca.hyperreal.bittydb

	object Main extends App
	{
	  // open a connection to an in-memory database using **GB 18030**,
	  //   the Chinese government standard character encoding (UTF-8 is the default)
	  val db = Connection.mem( "charset" -> "GB18030" )
	
	  // insert some documents into a collection named **test**,
	  //   which is created since it doesn't exist at this point,
	  //   adding a field **_id** with a UUID value to every document
	  db( "test" ).insert( Map("a" -> 1, "b" -> "first"), Map("a" -> 2, "b" -> "second"), Map("a" -> 3, "b" -> "third") )
	  
	  // show the contents of the entire database
	  println( db.root("test").get )
	
	  // query collection **test** for a document with field **a** less than 3
	  println( db( "test" ) find (_("a") < 3) toList )
	
	  // remove from collection **test** any document with field **a** equal to either 1 or 2
	  db( "test" ) remove (_("a") in Set(1, 2))
	  println( db.root("test").get )
	  
	  // update collection **test** such that any document with field **a** equal to 3
	  //   will have field **b** changed to "第三" (meaning 'third' in Chinese)
	  db( "test" ) update (_("a") === 3, "b" -> "第三")
	  println( db.root("test").get )
	}

The above example should output something very similar (the UUID's will be different) to

	List(Map(a -> 1, b -> first, _id -> 85fac459-2638-4524-b671-d35bb4fd1b86), Map(a -> 2, b -> second, _id -> 7484e63b-6598-4d6c-9bdd-f25c69a942f1), Map(a -> 3, b -> third, _id -> 543de2cb-5899-49a7-8f37-741f6fd9c4d5))
	List(Map(a -> 1, b -> first, _id -> 85fac459-2638-4524-b671-d35bb4fd1b86), Map(a -> 2, b -> second, _id -> 7484e63b-6598-4d6c-9bdd-f25c69a942f1))
	List(Map(a -> 3, b -> third, _id -> 543de2cb-5899-49a7-8f37-741f6fd9c4d5))
	List(Map(a -> 3, b -> 第三, _id -> 543de2cb-5899-49a7-8f37-741f6fd9c4d5))
	
	
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
	
Add the following to your build.sbt
	
	libraryDependencies += "ca.hyperreal" %% "bittydb" % "0.1"
