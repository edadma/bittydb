package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
//	db.root.set( "a" -> 123 )
	db.root.set( "b" -> List(1, 2, 3) )
	db.io.dump
	println( db.root.get )
}