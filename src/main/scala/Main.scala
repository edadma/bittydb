package ca.hyperreal.bittydb

import ca.hyperreal.lia.Math


object Main extends App
{
	val db = Connection.mem()

	println( db.root("test").get )
}