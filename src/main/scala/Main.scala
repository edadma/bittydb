package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
	db.io.dump
	println( db.root.get )
	
//	db.root.set( "asdf" -> 0x1234 )
//	db.root.set( "qwer" -> 0x5678 )
	db.root.set( "first large key value" -> "1" )
	db.io.dump
	println( db.root.get )
	
	db.root.set( "second large key value" -> "2" )
	db.io.dump
	println( db.root.get )
	
	println( db.root.remove( "second large key value" ) )
	db.io.dump
	println( db.root.get )
	
// 	db.root.set( "b" -> 0x7890 )
// 	db.io.dump
// 	println( db.root.get )
}