package ca.hyperreal.bittydb


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