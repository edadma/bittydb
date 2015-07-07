package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
	db.io.dump
	println( db.root.get )
	
 	db.root.set( "a" -> 123 )
 	db.root.set( "zxcv" -> 456 )
	db.io.dump
	println( db.root.get )
	
	println( db.root.remove( "zxcv" ) )
	db.io.dump
	println( db.root.get )
	
 	db.root.set( "b" -> 789 )
	db.io.dump
	println( db.root.get )
}