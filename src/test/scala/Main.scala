package xyz.hyperreal.bittydb

import scala.collection.mutable.ArrayBuffer
import util.Random._


object Main extends App {

	val db = Connection.mem( 'uuid -> false )
	val coll = db( "test" )
	val array = new ArrayBuffer[Map[String, Any]]

	for (_ <- 1 to 100) {
		val m = Map( rndAlpha -> nextInt(Int.MaxValue) )

		coll.insert( m )
		array += m
	}

	println( coll.set == array.toSet )
	println( db.length )

	for (_ <- 1 to 50) {
		val ind = nextInt( array.length )
		val doc = array( ind )

		array remove ind
		coll remove doc
	}

	println( coll.set == array.toSet )
	println( db.length )

	for (_ <- 1 to 50) {
		val m = Map( rndAlpha -> nextInt(Int.MaxValue) )

		coll.insert( m )
		array += m
	}

	println( coll.set == array.toSet )
	println( db.length )

	db.close

	def rndAlpha = new String( Array.fill( nextInt(17) )((nextInt('z' - 'a') + 'a').toChar) )

}