package xyz.hyperreal.bittydb

import collection.mutable.{HashSet, ArrayBuffer}

import scala.util.Random._

import org.scalatest._
import org.scalacheck.Gen
import prop.PropertyChecks


class RandomTests extends FreeSpec with PropertyChecks with Matchers {

	def rndAlpha = new String( Array.fill( nextInt(10) )((nextInt('z' - 'a') + 'a').toChar) )

	def rnd( s: collection.Set[Map[String, Any]] ): Map[String, Any] = {
		val a = Map( rndAlpha -> nextLong )

		if (s(a))
			rnd( s )
		else
			a
	}

	val ints = for (n <- Gen.choose(1, 10)) yield n

	"mem" in {
		forAll (ints) { _ =>
			val db = Connection.mem( 'uuid -> false, 'pwidth -> 2, 'cwidth -> 2 )
			val coll = db( "test" )
			val set = new HashSet[Map[String, Any]]

			for (_ <- 1 to 500) {
				val m = rnd( set )

				coll.insert( m )
				set += m
			}

			coll.set shouldEqual set

			for (_ <- 1 to 200) {
				val doc = set.head

				coll remove doc
				set -= doc
			}

			coll.set shouldEqual set

			for (_ <- 1 to 250) {
				val m = rnd( set )

				coll.insert( m )
				set += m
			}

			coll.set shouldEqual set
			db.close
		}
	}
	
}