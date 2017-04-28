package xyz.hyperreal.bittydb

import collection.mutable.ArrayBuffer
import util.Random._


object Main extends App {

	val db = Connection.mem( 'uuid -> false, 'bwidth -> 2, 'cwidth -> 3 )
	val coll = db( "test" )
	val m = Map("aaaaaaaa" -> 1)
	val n = Map("aaaa" -> 1)

	for (_ <- 1 to 1)
		coll insert m

	db.io.dump
	println( coll.list )
	coll remove m
	db.io.dump
	println( coll.list )

	for (_ <- 1 to 1)
		coll insert n

	db.io.dump

	try {
		println(coll.list)
	} catch {
		case e: Exception => println( e )
	}

	db.close

	def rndAlpha = new String( Array.fill( nextInt(15) )((nextInt('z' - 'a') + 'a').toChar) )

	def rndObject = {
		val res =
			Map(
				(for (_ <- 0 to nextInt(1))
					yield
						rndAlpha -> nextInt(Int.MaxValue)): _*)

		println( res )
		res
	}

}