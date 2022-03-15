package io.github.edadma.bittydb

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.HashSet
import util.Random.*

import pprint.*

object Main extends App:

  val db = Connection.mem()

  db("tables").insert(
    Map(
      "name" -> "t",
      "columns" ->
        Map(
          "id" ->
            Map(
              "meta" ->
                Map("type" -> "integer", "auto" -> true, "pk" -> true),
              "data" -> Nil,
            ),
          "a" ->
            Map(
              "meta" ->
                Map("type" -> "integer", "auto" -> false, "pk" -> false),
              "data" -> Nil,
            ),
        ),
    ),
  )

  pprintln(db("tables").find(_("name") === "t").toList)
