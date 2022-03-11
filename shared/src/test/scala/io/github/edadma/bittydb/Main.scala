package io.github.edadma.bittydb

import scala.collection.mutable.HashSet
import util.Random._

object Main extends App {

  val db = Connection.mem("uuid" -> false, "pwidth" -> 2, "cwidth" -> 2)
  val coll = db("DB")

  db.io.check

  def rndAlpha = new String(Array.fill(nextInt(64))((nextInt('z' - 'a') + 'a').toChar))

  def rnd(s: collection.Set[Map[String, Any]]): Map[String, Any] = {
    val a = Map(rndAlpha -> nextLong)

    if (s(a))
      rnd(s)
    else
      a
  }

  def prt(m: Map[String, Any]): Unit = {
    println(m map {
      case (k, v) => (k, v match { case i: Int => i.toHexString; case l: Long => l.toHexString; case _ => v })
    } mkString " -> ")
  }

  val set = new HashSet[Map[String, Any]]
  val insertions = 100

  println("insert")

  for (_ <- 1 to insertions) {
    val m = rnd(set)

//		prt( m )
    coll.insert(m)
    set += m
  }

  println((coll.set == set, db.io.size))
  db.io.check

  try { db.io.check } catch { case e: Exception => println(e); sys.exit(1) }

  println("\nremove")

  for (_ <- 1 to insertions / 2) {
    val doc = set.head

//		prt( doc )
    coll remove doc
    set -= doc
  }

  println((coll.set == set, db.io.size))
  db.io.check

  println("\ninsert")

  for (_ <- 1 to insertions / 2) {
    val m = rnd(set)

//		prt( m )
    coll.insert(m)
    set += m
  }

  println((coll.set == set, db.io.size))
  db.io.check
  db.close()

}
