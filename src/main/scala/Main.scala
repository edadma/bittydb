package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem( "charset" -> "GB18030" )

	db( "test" ).insert( Map("a" -> 1, "b" -> "first"), Map("a" -> 2, "b" -> "second"), Map("a" -> 3, "b" -> "third") )

	val res = db( "test" ).update( Map("a" -> Map("$lt" -> 3)), Map("$set" -> Map("a" -> 123, "b" -> 456)) )

	println( res )
	println( db.root("test").get )
}