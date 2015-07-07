package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
	db.io.dump
	println( db.root.get )
	
	db.root.set( "asdf" -> 123 )
	db.root.set( "zxcv" -> 456 )
	db.io.dump
	println( db.root.get )	
}