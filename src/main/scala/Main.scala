package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
	db.root.set( "a", List() )
	db.io.dump
	db.root.key( "a" ).prepend( 1, 2 )
	db.root.key( "a" ).prepend( 3, 4 )
	db.root.key( "a" ).append( 5 )
	db.root.key( "a" ).prepend( 6 )
	
	val it = db.root.key( "a" ).iterator
	
	it.drop(3).next.put( 7 )
	println( db.root.key( "a" ).valuesAs[Int].filter( _ < 4 ) toList )
}