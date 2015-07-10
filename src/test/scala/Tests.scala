package ca.hyperreal.bittydb

import org.scalatest._
import prop.PropertyChecks


class Tests extends FreeSpec with PropertyChecks with Matchers
{
	"set/remove" in
	{
	val db = Connection.mem()
	
		db.root.get shouldBe Map()
		
		db.root.set( "a", List(1, 2, 3) )
		db.root.get shouldBe Map( "a" -> List(1, 2, 3) )
		
		db.root.set( "b" -> 1234 )
		db.root.set( "bb" -> 234 )
		db.root.get shouldBe Map( "a" -> List(1, 2, 3), "b" -> 1234, "bb" -> 234 )
		
		db.root.remove( "b" ) shouldBe true
		db.root.get shouldBe Map( "a" -> List(1, 2, 3), "bb" -> 234 )

		db.root.set( "c" -> "qwerqwerqwer" )
		db.root.get shouldBe Map( "a" -> List(1, 2, 3), "bb" -> 234, "c" -> "qwerqwerqwer" )
		
		db.root.set( "d" -> 5678 )
		db.root.get shouldBe Map( "a" -> List(1, 2, 3), "bb" -> 234, "c" -> "qwerqwerqwer", "d" -> 5678 )
		
		db.root.remove( "asdf" ) shouldBe false
		db.root.get shouldBe Map( "a" -> List(1, 2, 3), "bb" -> 234, "c" -> "qwerqwerqwer", "d" -> 5678 )
	}
	
	"put" in
	{
	val db = Connection.mem()
	
		a [RuntimeException] should be thrownBy {db.root.put( List(1, 2, 3) )}
	}
	
	"append" in
	{
	val db = Connection.mem()
	
		db.root.set( "a", Nil )
		db.root.get shouldBe Map( "a" -> Nil )
		
		db.root.key( "a" ).append( 1 )
		db.root.get shouldBe Map( "a" -> List(1) )
		
		db.root.key( "a" ).append( "asdfasdfasdf" )
		db.root.get shouldBe Map( "a" -> List(1, "asdfasdfasdf") )
		
		db.root.key( "a" ).append( "qwerqwerqwer" )
		db.root.get shouldBe Map( "a" -> List(1, "asdfasdfasdf", "qwerqwerqwer") )
		
		db.root.key( "a" ).append( 3 )
		db.root.get shouldBe Map( "a" -> List(1, "asdfasdfasdf", "qwerqwerqwer", 3) )
	}
}