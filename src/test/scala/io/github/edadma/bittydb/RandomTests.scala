package xyz.hyperreal.bittydb

import org.scalatest.Tag

import scala.util.Random._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2, TableFor4}
import org.scalatest.tagobjects.{CPU, Disk}

import scala.collection.mutable

class RandomTests extends AnyFreeSpec with Matchers with TableDrivenPropertyChecks {

  def rndAlpha(max: Int) = new String(Array.fill(nextInt(max))((nextInt('z' - 'a') + 'a').toChar))

  def rnd(s: collection.Set[Map[String, Any]], max: Int): Map[String, Any] = {
    val a = Map(rndAlpha(max) -> nextLong)

    if (s(a))
      rnd(s, max)
    else
      a
  }

  object Always extends Tag("always run test")

  val trials: TableFor2[Int, Tag] =
    Table(
      ("trials", "tag"),
      (0, Disk),
      (1, Always),
      (2, CPU),
      (3, CPU)
    )
  val widths: TableFor4[Int, Int, Int, Int] =
    Table(
      ("pwidth", "cwidth", "insertions", "max length"),
      (1, 1, 3, 10),
      (1, 2, 3, 10),
      (1, 3, 3, 10),
      (2, 2, 200, 64),
      (2, 3, 200, 128),
      (2, 4, 200, 128),
      (3, 3, 500, 128),
      (3, 4, 500, 128),
      (3, 5, 500, 256),
      (4, 4, 1000, 128),
      (4, 5, 1000, 256),
      (4, 6, 1000, 256),
      (5, 5, 1000, 256),
      (5, 6, 1000, 256),
      (5, 7, 1000, 512),
      (5, 8, 1000, 512)
    )

  forAll(widths) { (pwidth, cwidth, insertions, max) =>
    forAll(trials) { (trial, tag) =>
      s"stress test trial $trial using pwidth of $pwidth and cwidth of $cwidth" taggedAs tag in {
        val db =
          if (trial == 0) {
            Database remove "test"
            Connection.disk("test", Symbol("uuid") -> false, Symbol("pwidth") -> pwidth, Symbol("cwidth") -> cwidth)
          } else
            Connection.mem(Symbol("uuid") -> false, Symbol("pwidth") -> pwidth, Symbol("cwidth") -> cwidth)
        val coll = db("test")
        val set = new mutable.HashSet[Map[String, Any]]

        db.io.check

        for (_ <- 1 to insertions) {
          val m = rnd(set, max)

          coll.insert(m)
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
          val m = rnd(set, max)

          coll.insert(m)
          set += m
        }

        db.io.check
        coll.set shouldEqual set
        db.close()
      }
    }
  }

}
