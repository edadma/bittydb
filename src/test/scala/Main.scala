package xyz.hyperreal.bittydb

import scala.collection.mutable.{ArrayBuffer, HashSet}
import util.Random._


object Main extends App {

	val db = Connection.mem( 'uuid -> false, 'pwidth -> 2, 'cwidth -> 3 )
	val coll = db( "DB" )
	def rndAlpha = new String( Array.fill( nextInt(10) )((nextInt('z' - 'a') + 'a').toChar) )

	def rnd( s: collection.Set[Map[String, Any]] ): Map[String, Any] = {
		val a = Map( rndAlpha -> nextLong )

		if (s(a))
			rnd( s )
		else
			a
	}

	val set = new HashSet[Map[String, Any]]

	for (_ <- 1 to 500) {
		val m = rnd( set )

		coll.insert( m )
		set += m
	}

	println( coll.set == set, db.io.size )

	for (_ <- 1 to 200) {
		val doc = set.head

		coll remove doc
		set -= doc
	}

	println( coll.set == set, db.io.size )

	for (_ <- 1 to 250) {
		val m = rnd( set )

		coll.insert( m )
		set += m
	}

	println( coll.set == set, db.io.size )
	db.close

}