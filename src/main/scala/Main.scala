package ca.hyperreal.bittydb

import ca.hyperreal.lia.Math


object Main extends App
{
	val db = Connection.mem()
	
//	db.root.set( "a" -> List(List(1), List("b")) )
//	db.root.set( "a" -> List(1, "b") )
	db.root.set( "a" -> 0x5A )
	db.io.dump
	println( db.root("a").get )
}