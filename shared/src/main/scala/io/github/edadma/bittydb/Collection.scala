package io.github.edadma.bittydb

import java.time.Instant
import java.util.UUID._
import scala.collection.mutable.ListBuffer

import io.github.edadma.dal.BasicDAL

object Collection {
  val QUERY_PREDICATES: Set[String] = Set("$eq", "$lt", "$gt", "$lte", "$gte", "$ne", "$in", "$nin")
  val UPDATE_OPERATORS: Set[String] = Set("$set", "$unset", "$datetime", "$timestamp")
}

class Collection(parent: Connection#Pointer, name: String) extends IOConstants {
  import Collection._

  private var c: Connection#Pointer = _

  check

  private def check =
    if (c eq null)
      parent.key(name) match {
        case None =>
          false
        case Some(p) =>
          require((p.typ & 0xf0) == LIST, "'collection' must be used in reference to a list")

          c = p
          true
      }
    else
      true

  private def create(): Unit =
    if (!check) {
      parent.set(name, Nil)
      c = parent(name)
    }

  def iterator: Iterator[Any] =
    if c eq null then Iterator.empty
    else c.elements

  def list: Seq[Any] = iterator.toList

  // iterator.toList

  def set: Set[Any] = iterator.toSet

  infix def filter(query: Map[_, _]): Iterator[Connection#Cursor] = {
    check
    c.cursor filter { m =>
      m.isMap && {
        val d = m.getAs[Map[Any, Any]]

        query forall {
          case (k, op: Map[_, _])
              if op.asInstanceOf[Map[Any, Any]].keysIterator forall (opk => QUERY_PREDICATES.exists(_ == opk)) =>
            op.head match {
              case ("$eq", v) => d get k contains v
              case ("$ne", v) => d get k exists (_ != v)
              case ("$lt", v) =>
                d get k exists (n => BasicDAL.relate(Symbol("<"), n.asInstanceOf[Number], v.asInstanceOf[Number]))
              case ("$lte", v) =>
                d get k exists (n => BasicDAL.relate(Symbol("<="), n.asInstanceOf[Number], v.asInstanceOf[Number]))
              case ("$gt", v) =>
                d get k exists (n => BasicDAL.relate(Symbol(">"), n.asInstanceOf[Number], v.asInstanceOf[Number]))
              case ("$gte", v) =>
                d get k exists (n => BasicDAL.relate(Symbol(">="), n.asInstanceOf[Number], v.asInstanceOf[Number]))
              case ("$in", v: Seq[Any])  => d get k exists v.contains
              case ("$nin", v: Seq[Any]) => d get k exists (!v.contains(_))
            }
          case (k, v) =>
            d get k contains v
        }
      }
    }
  }

  infix def filter(query: Connection#Cursor => Boolean): Iterator[Connection#Cursor] = {
    check
    c.cursor filter query
  }

  infix def find(cursor: Iterator[Connection#Cursor]): Iterator[Any] =
    if (check) cursor map (_.get)
    else Iterator.empty

  infix def find(query: Map[Any, Any] = Map()): Iterator[Any] = find(filter(query))

  infix def find(query: Connection#Cursor => Boolean): Iterator[Any] = find(filter(query))

  infix def remove(cursor: Iterator[Connection#Cursor]): Int =
    if (check) {
      var count = 0

      for (v <- cursor) {
        v.remove
        count += 1
      }

      count
    } else 0

  infix def remove(query: Map[Any, Any]): Int = remove(filter(query))

  def remove(query: Connection#Cursor => Boolean): Int = remove(filter(query))

  infix def update(cursor: Iterator[Connection#Cursor], updates: collection.Seq[UpdateOperator]): Int =
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
    } else 0

  infix def update(query: Connection#Cursor => Boolean, updates: (String, Any)*): Int =
    if (check) update(filter(query), updates map { case (field, update) => SetUpdateOperator(field, update) }) else 0

  infix def update(query: Map[Any, Any], updates: Any): Int = {
    val ops = new ListBuffer[UpdateOperator]

    updates match {
      case map: Map[_, _] if map.asInstanceOf[Map[String, Any]].keysIterator forall (_.isInstanceOf[String]) =>
        if (map.asInstanceOf[Map[String, Any]].keysIterator exists UPDATE_OPERATORS)
          for (update <- map.asInstanceOf[Map[String, Any]])
            update match {
              case ("$set", fields: Map[_, _]) =>
                ops ++= fields map { case (field, value) => SetUpdateOperator(field, value) }
//							case ("$unset", fields: Map[_, _]) => ops ++= fields map {case (field, value) => UnsetUpdateOperator( field )}
              case _ => sys.error("udpate: 'updates' must be either all update operators, or the update value itself")
            }
        else
          ops += DocumentUpdateOperator(map)
      case _ => ops += DocumentUpdateOperator(updates)
    }

    if (check) update(filter(query), ops) else 0
  }

  def insert(documents: Map[Any, Any]*): Unit = {
    create()

    for (d <- documents)
      c.insert(if ((d contains "_id") || !parent.connection.io.uuidOption) d else d + ("_id" -> randomUUID))
  }

  override def toString: String = "collection " + name

  abstract class UpdateOperator
  case class DocumentUpdateOperator(value: Any) extends UpdateOperator
  case class SetUpdateOperator(field: Any, value: Any) extends UpdateOperator
  case class UnsetUpdateOperator(field: Any) extends UpdateOperator
  case class DateUpdateOperator(field: Any) extends UpdateOperator
  case class TimestampUpdateOperator(field: Any) extends UpdateOperator
}
