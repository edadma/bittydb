package ca.hyperreal.bittydb

import java.util.UUID._

import ca.hyperreal.lia.Math


object Collection {
	val operators = Set( "$eq", "$lt", "$gt", "$lte", "$gte", "$ne", "$in", "$nin" )
}

class Collection( parent: Connection#Pointer, name: String ) extends IOConstants {
	type Document = Map[_, _]
	
	private var c: Connection#Pointer = null
	
	check
	
	private def check =
		if (c eq null)
			parent.key( name ) match {
				case None =>
					false
				case Some( p ) =>
					require( (p.kind&0xF0) == ARRAY, "'collection' must be used in reference to an array" )
					
					c = p
					true
			}
		else
			true
	
	private def create =
		if (!check)
		{
			parent.set( name, Nil )
			c = parent( name )
		}
	
	def filter( query: Map[_, _] ) =
		c.iterator filter {
			m =>
				(m.kind&0xF0) == OBJECT && {
					val d = m.getAs[Map[Any, Any]]
					
					query forall {
						case (k, op: Map[String, Any]) if op.keysIterator forall (Collection.operators contains _) =>
							op.head match {
								case ("$eq", v) => d get k exists (_ == v)
								case ("$ne", v) => d get k exists (_ != v)
								case ("$lt", v) => d get k exists (Math.predicate( '<, _, v ))
								case ("$lte", v) => d get k exists (Math.predicate( '<=, _, v ))
								case ("$gt", v) => d get k exists (Math.predicate( '>, _, v ))
								case ("$gte", v) => d get k exists (Math.predicate( '>=, _, v ))
								case ("$in", v: Seq[Any]) => d get k exists (v contains _)
								case ("$nin", v: Seq[Any]) => d get k exists (!v.contains(_))
							}
						case (k, v) =>
							d get k exists (_ == v)
					}
				}
		}
	
	def filter( query: Connection#Cursor => Boolean ) = c.iterator filter query
	
	def find( query: Document = Map() ) = {
		if (check) {
			filter (query) map (_.get)
		} else
			Iterator.empty
	}
	
	def find( query: Connection#Cursor => Boolean ) = {
		if (check) {
			filter (query) map (_.get)
		} else
			Iterator.empty
	}
	
	def remove( query: Document ) = {
		if (check) {
			var count = 0
			
			for (v <- filter (query)) {
				v.remove
				count += 1
			}
			
			count
		} else
			0
	}
	
	def insert( documents: Document* ) {
		create
		
		for (d <- documents.asInstanceOf[Seq[Map[Any, Any]]])
			c.append( if (d contains "_id") d else d + ("_id" -> randomUUID) )
	}
}