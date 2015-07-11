package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
//	db.root.set( "a", List("abc") )
// 	db.root.set( "a", "asdfasdfasdf" )
// 	db.io.dump
// 	println( db.root.get )
	
	db.root.put( Map("a" -> 0x12345678) )
//	db.root.set( "b" -> 1234 )
	db.io.dump
	println( db.root.get )
	
// 	db.root.set( "c", 5678 )
// 	db.io.dump
// 	println( db.root.get )
// 	
// 	db.root.key( "a" ).append( "def" )
// 	db.io.dump
// 	println( db.root.get )
// 	
// 	db.root.key( "a" ).append( "ghi" )
// 	db.io.dump
// 	println( db.root.get )
}