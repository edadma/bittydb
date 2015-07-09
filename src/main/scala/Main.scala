package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
//	db.root.set( "a" -> 123 )
	db.root.set( "time" -> "asdfasdf" )
	db.io.dump
	println( db.root.get )
}