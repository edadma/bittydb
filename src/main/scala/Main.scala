package xyz.hyperreal.bittydb

import xyz.hyperreal.lia.Math


object Main extends App {
	IO.bwidth_default = 1
	IO.cwidth_default = 1
	
	var db = Connection.mem()
	
	db.io.dump
	println( db.root.get )
	db.root.set( "a" -> List(0x12) )
// 	db.root.set( "b" -> 0x34 )
 	db.io.dump
	println( db.root.get )
// 	println( db.io.objectIterator(db.rootPtr).next.toHexString )
}