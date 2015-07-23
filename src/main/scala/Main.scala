package ca.hyperreal.bittydb

import ca.hyperreal.lia.Math


object Main extends App {
	IO.bwidth_default = 1
	IO.cwidth_default = 1
	
	var db = Connection.mem()
	
	db.root.set( "a" -> "bc" )
	println( db.root.get )
	db.io.dump
	
	db.root.remove( "a" )
	println( db.root.get )
	db.io.dump
	
	db.root.set( "a" -> "de" )
	println( db.root.get )
	db.io.dump
}