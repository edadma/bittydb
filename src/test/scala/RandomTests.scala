package xyz.hyperreal.bittydb

import collection.mutable.HashSet
import scala.util.Random._
import org.scalatest._
import org.scalatest.tagobjects.{CPU, Disk}
import prop.{PropertyChecks, TableDrivenPropertyChecks}


class RandomTests extends FreeSpec with PropertyChecks with Matchers with TableDrivenPropertyChecks {

	def rndAlpha = new String( Array.fill( nextInt(10) )((nextInt('z' - 'a') + 'a').toChar) )

	def rnd( s: collection.Set[Map[String, Any]] ): Map[String, Any] = {
		val a = Map( rndAlpha -> nextLong )

		if (s(a))
			rnd( s )
		else
			a
	}

	object Always extends Tag( "always run test" )

	val trials =
		Table(
			("trials", "tag"),
			(0, Disk),
			(1, Always),
			(2, CPU),
			(3, CPU)
		)
	val widths =
		Table(
			("pwidth", "cwidth", "insertions"),
			(1, 1, 3),
			(1, 2, 3),
			(1, 3, 3),
			(2, 2, 500),
			(2, 3, 500),
			(2, 4, 500),
			(3, 3, 1000),
			(3, 4, 1000),
			(3, 5, 1000),
			(4, 4, 1000),
			(4, 5, 1000),
			(4, 6, 1000),
			(5, 5, 2000),
			(5, 6, 2000),
			(5, 7, 2000),
			(5, 8, 2000)
		)

	forAll (widths) { (pwidth, cwidth, insertions) =>
		forAll (trials) { ( trial, tag ) =>
			s"stress test trial $trial using pwidth of $pwidth and cwidth of $cwidth" taggedAs tag in {
				val db =
					if (trial == 0) {
						Database remove "test"
						Connection.disk( "test", 'uuid -> false, 'pwidth -> pwidth, 'cwidth -> cwidth )
					} else
						Connection.mem( 'uuid -> false, 'pwidth -> pwidth, 'cwidth -> cwidth )
				val coll = db( "test" )
				val set = new HashSet[Map[String, Any]]

				db.io.check

				for (_ <- 1 to insertions) {
					val m = rnd( set )

					coll.insert( m )
					set += m
				}

				db.io.check
				coll.set shouldEqual set

				for (_ <- 1 to insertions / 2) {
					val doc = set.head

					coll remove doc
					set -= doc
				}

				db.io.check
				coll.set shouldEqual set

				for (_ <- 1 to insertions / 2) {
					val m = rnd( set )

					coll.insert( m )
					set += m
				}

				db.io.check
				coll.set shouldEqual set
				db.close
			}
		}
	}

}