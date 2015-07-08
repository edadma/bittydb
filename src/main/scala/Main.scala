package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
	db.io.dump
	println( db.root.get )
	
	db.root.set( "asdf" -> 0x1234 )
//	db.root.set( "zxcv" -> "this is a test" )
	db.io.dump
	println( db.root.get )
	
// 	println( db.root.remove( "zxcv" ) )
// 	db.io.dump
// 	println( db.root.get )
	
//  	db.root.set( "b" -> 789 )
// 	db.io.dump
// 	println( db.root.get )
}