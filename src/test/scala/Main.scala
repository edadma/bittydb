package xyz.hyperreal.bittydb

//import collection.mutable.ArrayBuffer
//import util.Random._


object Main extends App {

	val db = Connection.mem( 'uuid -> false, 'pwidth -> 2, 'cwidth -> 3 )
	val coll = db( "DB" )
	val m = Map("aaaaaaa" -> 1)
	val n = Map("aaaa" -> 1)

	db.io.dump
	db.io.check

	coll insert m
	db.io.dump
	println( coll.list )

	coll remove m
	db.io.dump
	println( coll.list )

	coll insert n
	db.io.dump

	try {
		println(coll.list)
	} catch {
		case e: Throwable => println( e )
	}

	db.close

}