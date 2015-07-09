package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
//	db.root.set( "a" -> 123 )
	db.root.set( "b" -> Map("c" -> 3, "d" -> 4) )
	db.root.set( "b" -> Map("c" -> 3, "d" -> 4) )
	db.io.dump
	println( db.root.get )
// 	println( db.root.key("b").get )
// 	println( db.root.key("b").key("d").get )
}