package ca.hyperreal.bittydb

import java.util.UUID._


class Collection( parent: Connection#Pointer, name: String ) extends IOConstants {
	type Document = Map[Any, Any]
	
	private var c: Connection#Pointer = null
	
	check
	
	private def check =
		if (c eq null)
			parent.keyOption( name ) match {
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
			c = parent key name
		}
		
	private def empty = Iterator.empty
	
	def find( query: Document ) = {
		if (check) {
			c.iterator filter {
				m =>
					(m.kind&0xF0) == OBJECT && {
						val d = m.getAs[Document]
						
						query forall {case (k,v) => d get (k) exists (_ == v)}
					}
			} map (_.get)
		} else
			empty
	}
	
	def insert( documents: Document* ) {
		create
		
		for (d <- documents)			
			c.append( if (d contains "_id") d else d + ("_id" -> randomUUID) )
	}
}