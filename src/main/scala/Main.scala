package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
	db.root.set( "a" -> List("abc") )
	db.io.dump
	println( db.root.get )
	
	db.root.set( "b" -> 1234 )
	db.io.dump
	println( db.root.get )
	
	db.root.key( "a" ).append( "def" )
	db.io.dump
	println( db.root.get )
}