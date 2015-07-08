package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
	db.io.dump
	println( db.root.get )
	
//	db.root.set( "asdf" -> 0x1234 )
//	db.root.set( "qwer" -> 0x5678 )
	db.root.set( "zxcv" -> "this is a test" )
	db.io.dump
	println( db.root.get )
	
// 	println( db.root.remove( "qwer" ) )
// 	db.io.dump
// 	println( db.root.get )
	
// 	db.root.set( "b" -> 0x7890 )
// 	db.io.dump
// 	println( db.root.get )
}