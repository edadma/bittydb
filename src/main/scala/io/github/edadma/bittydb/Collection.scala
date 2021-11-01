package io.github.edadma.bittydb

import java.util.UUID._
import java.time.Instant

import collection.mutable.ListBuffer

import xyz.hyperreal.lia.Math

object Collection {
  val QUERY_PREDICATES = Set("$eq", "$lt", "$gt", "$lte", "$gte", "$ne", "$in", "$nin")
  val UPDATE_OPERATORS = Set("$set", "$unset", "$datetime", "$timestamp")
}

class Collection(parent: Connection#Pointer, name: String) extends IOConstants {
  import Collection._

  type Document = Map[_, _]

  private var c: Connection#Pointer = _

  check

  private def check =
    if (c eq null)
      parent.key(name) match {
        case None =>
          false
        case Some(p) =>
          require((p.kind & 0xF0) == ARRAY, "'collection' must be used in reference to an array")

          c = p
          true
      } else
      true

  private def create(): Unit =
    if (!check) {
      parent.set(name, Nil)
      c = parent(name)
    }

  def iterator: Iterator[Any] = c.elements

  def list: Seq[Any] = iterator.toList

  //iterator.toList

  def set: Set[Any] = iterator.toSet

  def filter(query: Map[_, _]): Iterator[Connection#Cursor] = {
    check
    c.cursor filter { m =>
      (m.kind & 0xF0) == OBJECT && {
        val d = m.getAs[Map[Any, Any]]

        query forall {
          case (k, op: Map[String, Any]) if op.keysIterator forall (QUERY_PREDICATES contains) =>
            op.head match {
              case ("$eq", v)            => d get k contains v
              case ("$ne", v)            => d get k exists (_ != v)
              case ("$lt", v)            => d get k exists (Math.predicate('<, _, v))
              case ("$lte", v)           => d get k exists (Math.predicate('<=, _, v))
              case ("$gt", v)            => d get k exists (Math.predicate('>, _, v))
              case ("$gte", v)           => d get k exists (Math.predicate('>=, _, v))
              case ("$in", v: Seq[Any])  => d get k exists (v contains)
              case ("$nin", v: Seq[Any]) => d get k exists (!v.contains(_))
            }
          case (k, v) =>
            d get k contains v
        }
      }
    }
  }

  def filter(query: Connection#Cursor => Boolean): Iterator[Connection#Cursor] = {
    check
    c.cursor filter query
  }

  def find(cursor: Iterator[Connection#Cursor]): Iterator[Any] =
    if (check) {
      cursor map (_.get)
    } else
      Iterator.empty

  def find(query: Document = Map()): Iterator[Any] = find(filter(query))

  def find(query: Connection#Cursor => Boolean): Iterator[Any] = find(filter(query))

  def remove(cursor: Iterator[Connection#Cursor]): Int =
    if (check) {
      var count = 0

      for (v <- cursor) {
        v.remove
        count += 1
      }

      count
    } else
      0

  def remove(query: Document): Int = remove(filter(query))

  def remove(query: Connection#Cursor => Boolean): Int = remove(filter(query))

  def update(cursor: Iterator[Connection#Cursor], updates: Seq[UpdateOperator]): Int =
    if (check) {
      var count = 0

      for (c <- cursor) {
        updates foreach {
          case DocumentUpdateOperator(value)   => c.put(value)
          case SetUpdateOperator(field, value) => c(field).put(value)
          case UnsetUpdateOperator(field)      => c.remove(field)
//					case DateUpdateOperator(field) =>
          case TimestampUpdateOperator(field) => c(field).put(Instant.now)
        }

        count += 1
      }

      count
    } else
      0

  def update(query: Connection#Cursor => Boolean, updates: (String, Any)*): Int =
    if (check) update(filter(query), updates map { case (field, update) => SetUpdateOperator(field, update) }) else 0

  def update(query: Document, updates: Any): Int = {
    val ops = new ListBuffer[UpdateOperator]

    updates match {
      case map: Map[String, Any] if map.keysIterator forall (_.isInstanceOf[String]) =>
        if (map.keysIterator exists UPDATE_OPERATORS)
          for (update <- map)
            update match {
              case ("$set", fields: Map[_, _]) =>
                ops ++= fields map { case (field, value) => SetUpdateOperator(field, value) }
//							case ("$unset", fields: Map[_, _]) => ops ++= fields map {case (field, value) => UnsetUpdateOperator( field )}
              case _ => sys.error("udpate: 'updates' must be either all update operators, or the update value itself")
            } else
          ops += DocumentUpdateOperator(map)
      case _ => ops += DocumentUpdateOperator(updates)
    }

    if (check) update(filter(query), ops) else 0
  }

  def insert(documents: Document*): Unit = {
    create()

    for (d <- documents.asInstanceOf[Seq[Map[Any, Any]]])
      c.append(if ((d contains "_id") || !parent.connection.io.uuidOption) d else d + ("_id" -> randomUUID))
  }

  override def toString: String = "collection " + name

  abstract class UpdateOperator
  case class DocumentUpdateOperator(value: Any) extends UpdateOperator
  case class SetUpdateOperator(field: Any, value: Any) extends UpdateOperator
  case class UnsetUpdateOperator(field: Any) extends UpdateOperator
  case class DateUpdateOperator(field: Any) extends UpdateOperator
  case class TimestampUpdateOperator(field: Any) extends UpdateOperator
}
