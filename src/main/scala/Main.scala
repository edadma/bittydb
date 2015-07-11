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
	println( db.root.key( "a" ).iterator map (_.get) toList )
}