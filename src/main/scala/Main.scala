package ca.hyperreal.bittydb

import ca.hyperreal.lia.Math


object Main extends App
{
	val db = Connection.mem()
	
	db.root.set( "a" -> List(List(1), List("bc")) )
	db.io.dump
	println( db.root("a").get )
}