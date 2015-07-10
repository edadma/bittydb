package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
		db.root.set( "a", Nil )
		
		db.root.key( "a" ).append( 1 )
		db.io.dump
		println( db.root.get )
		
// 		db.root.key( "a" ).append( "asdfasdfasdf" )
// 		db.io.dump
// 		println( db.root.get )
		
// 		db.root.key( "a" ).append( "qwerqwerqwer" )
// 		db.io.dump
// 		println( db.root.get )
// 		
// 		db.root.key( "a" ).append( 3 )
// 		db.io.dump
// 		println( db.root.get )
}