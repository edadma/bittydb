package ca.hyperreal.bittydb

import ca.hyperreal.lia.Math


object Main extends App
{
	val db = Connection.mem()
	
	db.root.set( "test" -> List(Map("b" -> "first"), Map("b" -> "asdfasdfa")) )
	db.io.dump
	println( db.root("test").get )
}