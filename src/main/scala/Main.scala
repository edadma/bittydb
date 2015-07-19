package ca.hyperreal.bittydb

import ca.hyperreal.lia.Math


object Main extends App
{
	val db = Connection.mem( 'charset -> "GB18030" )

	db.root.set( "test" -> List(Map("a" -> 1, "b" -> "first"), Map("a" -> 2, "b" -> "second"), Map("a" -> 3, "b" -> "third")) )
	println( db.root.get )

	val res = db( "test" ).update( Map("a" -> Map("$lt" -> 3)), Map("$set" -> Map("a" -> 123, "b" -> 456)) )

	println( res )
	println( db.root("test").get )
}