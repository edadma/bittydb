package xyz.hyperreal.bittydb

import scala.collection.mutable.{ArrayBuffer, HashSet}
import util.Random._


object Main extends App {

	val db = Connection.mem( 'uuid -> false, 'pwidth -> 1, 'cwidth -> 1 )
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
	val insertions = 3

	for (_ <- 1 to insertions) {
		val m = rnd( set )

		coll.insert( m )
		set += m
	}

	println( coll.set == set, db.io.size )
	db.io.dump

	try {db.io.check} catch {case e: Exception => println( e ); sys.exit(1)}

	for (_ <- 1 to insertions/2) {
		val doc = set.head

		coll remove doc
		set -= doc
	}

	println( coll.set == set, db.io.size )

	for (_ <- 1 to insertions/2) {
		val m = rnd( set )

		coll.insert( m )
		set += m
	}

	println( coll.set == set, db.io.size )
	db.close

}