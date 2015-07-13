package ca.hyperreal.bittydb


object Main extends App
{
	  // open a connection to an in-memory database using **GB 18030**,
	  //   the Chinese government standard character encoding (UTF-8 is the default)
	  val db = Connection.mem( "charset" -> "GB18030" )
	
	  // insert some documents into a collection named **test**,
	  //   which is created since it doesn't exist at this point,
	  //   adding a field **_id** with a UUID value to every document
	  db( "test" ).insert( Map("a" -> 1, "b" -> "first"), Map("a" -> 2, "b" -> "second"), Map("a" -> 3, "b" -> "third") )
	  
	  // show the contents of the entire database
	  println( db.root("test").get )
	
	  // query collection **test** for a document with field **a** less than 3
	  println( db( "test" ) find (_("a") < 3) toList )
	
	  // remove from collection **test** any document with field **a** equal to either 1 or 2
	  db( "test" ) remove (_("a") in Set(1, 2))
	  println( db.root("test").get )
	  
	  // update collection **test** such that any document with field **a** equal to 3
	  //   will have field **b** changed to "第三" (meaning 'third' in Chinese)
	  db( "test" ) update (_("a") === 3, "b" -> "第三")
	  println( db.root("test").get )
}