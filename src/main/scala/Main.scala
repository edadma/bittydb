package ca.hyperreal.bittydb

import ca.hyperreal.lia.Math


object Main extends App
{
	val db = Connection.mem( "charset" -> "GB18030" )

		db.root.set( "a" -> Nil )
		println( db.root( "a" ).arrayIterator.isEmpty )
		
// 		db.root( "a" ).prepend( 1, 2 )
// 		db.root( "a" ).prepend( 3, 4 )
		db.root( "a" ).append( 5 )
//		db.root( "a" ).prepend( 6 )
//		println( db.root( "a" ).get )
		
// 		db.root( "a" ).arrayIterator.drop(3).next.put( "happy" )
// 		println( db.root( "a" ).get )
		println( db.root( "a" ).members.toList )
}