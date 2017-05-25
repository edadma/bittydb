package xyz.hyperreal.bittydb

import scala.collection.mutable.HashSet
import util.Random._


object Main extends App {

	val db = Connection.mem( 'uuid -> false, 'pwidth -> 1, 'cwidth -> 1 )

	println( db.root.set( "a" -> List(0x11, 0x22, 0x33) ) )
	println( db.root.get )

	println( db.root.set( "b" -> 0x44 ) )
	println( db.root.set( "c" -> 0x55 ) )
	println( db.root.get )
	println( db.root.remove( "b" ) )
	println( db.root.get )

//	val coll = db( "D" )
//
//	def rndAlpha = new String( Array.fill( 1 )((nextInt('z' - 'a') + 'a').toChar) )
//
//	def rnd( s: collection.Set[Map[String, Any]] ): Map[String, Any] = {
//		val a = Map( rndAlpha -> Vector(0x55) )
//
//		if (s(a))
//			rnd( s )
//		else
//			a
//	}
//
//	def prt( m: Map[String, Any] ): Unit = {
//		println( m map {case (k, v) => (k, v match {case i: Int => i.toHexString; case l: Long => l.toHexString; case _ => v})} mkString " -> " )
//	}
//
//	val set = new HashSet[Map[String, Any]]
//	val insertions = 1
//	val deletions = 1
//
//	println( "insert" )
//
//	for (_ <- 1 to insertions) {
//		val m = rnd( set )
//
//		prt( m )
//		coll.insert( m )
//		set += m
//	}
//
//	db.io.dump
//
//	try {db.io.check} catch {case e: Exception => println( e ); sys.exit(1)}
//
//	println( coll.set )
//	println( coll.set == set, db.io.size )
//
//	println( "\nremove" )
//
//	for (_ <- 1 to deletions) {
//		val doc = set.head
//
//		prt( doc )
//		coll remove doc
//		set -= doc
//	}
//
//	db.io.dump
//	db.io.check
//	println( coll.set == set, db.io.size )
//
//	println( "\ninsert" )
//
//	for (_ <- 1 to deletions) {
//		val m = rnd( set )
//
//		prt( m )
//		coll.insert( m )
//		set += m
//	}
//
//	db.io.dump
//	db.io.check
//	println( coll.set == set, db.io.size )
	db.close

}