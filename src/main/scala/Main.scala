package ca.hyperreal.bittydb


object Main extends App
{
	val db = Connection.mem()
	
	db.io.dump
}