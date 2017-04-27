package xyz.hyperreal.bittydb

import collection.mutable.ArrayBuffer

import scala.util.Random._

import org.scalatest._
import org.scalacheck.Gen
import prop.PropertyChecks


class RandomTests extends FreeSpec with PropertyChecks with Matchers {

	def rndAlpha = new String( Array.fill( nextInt(17) )((nextInt('z' - 'a') + 'a').toChar) )

	val ints = for (n <- Gen.choose(1, 10)) yield n

	"mem" in {
		forAll (ints) { _ =>
			val db = Connection.mem( 'uuid -> false )
			val coll = db( "test" )
			val array = new ArrayBuffer[Map[String, Any]]

			for (_ <- 1 to 100) {
				val m = Map( rndAlpha -> nextInt(Int.MaxValue) )

				coll.insert( m )
				array += m
			}

			coll.set shouldEqual array.toSet

			for (_ <- 1 to 50) {
				val ind = nextInt( array.length )
				val doc = array( ind )

				array remove ind
				coll remove doc
			}

			coll.set shouldEqual array.toSet

			for (_ <- 1 to 50) {
				val m = Map( rndAlpha -> nextInt(Int.MaxValue) )

				coll.insert( m )
				array += m
			}

			coll.set shouldEqual array.toSet

			db.close
		}
	}
	
}