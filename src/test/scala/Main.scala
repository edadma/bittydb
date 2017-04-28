package xyz.hyperreal.bittydb

import scala.collection.mutable.ArrayBuffer
import util.Random._


object Main extends App {

	val db = Connection.mem( 'uuid -> false )
	val coll = db( "test" )
	val array = new ArrayBuffer[Map[String, Any]]

	db.io.dump

	for (_ <- 1 to 1) {
		val m = Map("aaaaaaaaa" -> 0x55aa55aa)//rndObject

		coll.insert( m )
		array += m
	}

	db.io.dump
	println( coll.set == array.toSet )
	println( db.length )

	for (_ <- 1 to 1) {
		val ind = nextInt( array.length )
		val doc = array( ind )

		array remove ind
		coll remove doc
	}

	println( coll.set == array.toSet )
	println( db.length )

//	for (_ <- 1 to 1) {
//		val m = Map("aaaaaaaaaaaaaaa" -> 0x55aa55aa)//rndObject
//
//		println( m )
//		coll.insert( m )
//		array += m
//	}
//
//	println( coll.set == array.toSet )
//	println( db.length )

	db.close

	def rndAlpha = new String( Array.fill( nextInt(17) )((nextInt('z' - 'a') + 'a').toChar) )

	def rndObject = //Map( "abababababababab" -> 0x5a5a5a5a )
		Map(
			(for (_ <- 0 to nextInt( 1 ))
				yield
					rndAlpha -> nextInt( Int.MaxValue )): _* )

}