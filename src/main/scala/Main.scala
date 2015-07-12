package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
	db.root.set( "test", List(1, Map("a" -> 1, "b" -> "first"), Map("a" -> 2, "b" -> "second"), Map("a" -> 3, "b" -> "third")) )
	println( db.root.key("test").get )
	
	db.collection( "test" ).insert( Map("a" -> 4, "b" -> "fourth") )
	println( db.root.key("test").get )
	
	println( db.collection( "test" ) remove (Map("a" -> Map("$in" -> Seq(1, 3)))) )
	println( db.root.key("test").get )
}