package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
	db.root.set( "test", List(1, Map("a" -> 1, "b" -> "first"), Map("a" -> 2, "b" -> "second"), Map("a" -> 3, "b" -> "third")) )
	println( db.root("test").get )
	
	db.collection( "test" ).insert( Map("a" -> 4, "b" -> "fourth") )
	println( db.root("test").get )
	
	println( db.collection( "test" ) find () toList )
	
	println( db.collection( "test" ) find (Map("a" -> Map("$in" -> Seq(1, 3))): Map[Any, Any]) toList )
	println( db.collection( "test" ) find (_("a") === 1) toList )
}