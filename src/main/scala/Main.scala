package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
		db.root.set( "a", Nil )
		db.root.key( "a" ).prepend( 1, 2 )
		db.io.dump
		println( db.root.get )
		db.root.key( "a" ).prepend( 3, 4 )
		db.io.dump
		println( db.root.get )
}