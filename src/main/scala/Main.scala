package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
//	db.root.set( "a", List("abc") )
	db.root.set( "a", "asdfasdfasdf" )
	db.io.dump
	println( db.root.get )
	
//	db.root.put( Map("a" -> List(1, 2, 3)) )
	db.root.set( "b" -> 1234 )
	db.root.set( "bb" -> 234 )
	db.io.dump
	println( db.root.get )
	
	db.root.remove( "b" )
	db.io.dump
	println( db.root.get )

	db.root.set( "c" -> "qwerqwerqwer" )
	db.io.dump
	println( db.root.get )
	
	db.root.set( "d" -> 5678 )
	db.io.dump
	println( db.root.get )
	
	db.root.set( "d" -> "wow" )
	db.io.dump
	println( db.root.get )

// 	db.root.key( "a" ).append( "def" )
// 	db.io.dump
// 	println( db.root.get )
// 	
// 	db.root.key( "a" ).append( "ghi" )
// 	db.io.dump
// 	println( db.root.get )
}