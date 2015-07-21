package ca.hyperreal.bittydb

import ca.hyperreal.lia.Math


object Main extends App
{
	val db = Connection.mem( 'bwidth -> 1, 'cwidth -> 1 )
	
	db.root.set( "a" -> List(List("bc"), List(1)) )
	db.io.dump
	println( db.root("a").get )
}