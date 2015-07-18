package ca.hyperreal.bittydb

import org.scalatest._
import prop.PropertyChecks


class Tests extends FreeSpec with PropertyChecks with Matchers
{
	"set/remove" in
	{
	val db = Connection.mem()
	
		db.root.get shouldBe Map()
		
		db.root.set( "a" -> List(1, 2, 3) ) shouldBe false
		db.root.get shouldBe Map( "a" -> List(1, 2, 3) )
		
		db.root.set( "b" -> 1234 ) shouldBe false
		db.root.set( "bb" -> 234 ) shouldBe false
		db.root.get shouldBe Map( "a" -> List(1, 2, 3), "b" -> 1234, "bb" -> 234 )
		
		db.root.remove( "b" ) shouldBe true
		db.root.get shouldBe Map( "a" -> List(1, 2, 3), "bb" -> 234 )

		db.root.set( "c" -> "qwerqwerqwer" ) shouldBe false
		db.root.get shouldBe Map( "a" -> List(1, 2, 3), "bb" -> 234, "c" -> "qwerqwerqwer" )

		db.root.set( "e" -> Map("x" -> "asdfasdfasdf", "y" -> "zxcvzxcvzxcv") ) shouldBe false
		db.root.get shouldBe Map( "a" -> List(1, 2, 3), "bb" -> 234, "c" -> "qwerqwerqwer", "e" -> Map("x" -> "asdfasdfasdf", "y" -> "zxcvzxcvzxcv") )
		
		db.root.set( "d" -> 5678 ) shouldBe false
		db.root.get shouldBe Map( "a" -> List(1, 2, 3), "bb" -> 234, "c" -> "qwerqwerqwer", "d" -> 5678, "e" -> Map("x" -> "asdfasdfasdf", "y" -> "zxcvzxcvzxcv") )
		
		db.root.remove( "asdf" ) shouldBe false
		db.root.get shouldBe Map( "a" -> List(1, 2, 3), "bb" -> 234, "c" -> "qwerqwerqwer", "d" -> 5678, "e" -> Map("x" -> "asdfasdfasdf", "y" -> "zxcvzxcvzxcv") )
		
		db.root.set( "d" -> "wow" ) shouldBe true
		db.root.get shouldBe Map( "a" -> List(1, 2, 3), "bb" -> 234, "c" -> "qwerqwerqwer", "d" -> "wow", "e" -> Map("x" -> "asdfasdfasdf", "y" -> "zxcvzxcvzxcv") )
	}
	
	"put" in
	{
	val db = Connection.mem()
	
		a [RuntimeException] should be thrownBy {db.root.put( List(1, 2, 3) )}
	}
	
	"append/length" in
	{
	val db = Connection.mem()
	
		db.root.set( "a" -> Nil )
		db.root.get shouldBe Map( "a" -> Nil )
		db.root( "a" ).length shouldBe 0
		
		db.root( "a" ).append( 1 )
		db.root.get shouldBe Map( "a" -> List(1) )
		db.root( "a" ).length shouldBe 1
		
		db.root( "a" ).append( 2, 2.5 )
		db.root.get shouldBe Map( "a" -> List(1, 2, 2.5) )
		db.root( "a" ).length shouldBe 3
		
		db.root( "a" ).append( "asdfasdfasdf" )
		db.root.get shouldBe Map( "a" -> List(1, 2, 2.5, "asdfasdfasdf") )
		db.root( "a" ).length shouldBe 4
		
		db.root( "a" ).append( "qwerqwerqwer" )
		db.root.get shouldBe Map( "a" -> List(1, 2, 2.5, "asdfasdfasdf", "qwerqwerqwer") )
		db.root( "a" ).length shouldBe 5
		
		db.root( "a" ).append( 3 )
		db.root.get shouldBe Map( "a" -> List(1, 2, 2.5, "asdfasdfasdf", "qwerqwerqwer", 3) )
		db.root( "a" ).length shouldBe 6
	}
	
	"prepend/length" in
	{
	val db = Connection.mem()
	
		db.root.set( "a" -> Nil )
		db.root( "a" ).prepend( 1 )
		db.root.get shouldBe Map( "a" -> List(1) )
		db.root( "a" ).length shouldBe 1
		db.root( "a" ).prepend( 2 )
		db.root.get shouldBe Map( "a" -> List(2, 1) )
		db.root( "a" ).length shouldBe 2
	}
	
	"iterator" in
	{
	val db = Connection.mem()
	
		db.root.set( "a" -> Nil )
		db.root( "a" ).arrayIterator.isEmpty shouldBe true
		
		db.root( "a" ).prepend( 1, 2 )
		db.root( "a" ).prepend( 3, 4 )
		db.root( "a" ).append( 5 )
		db.root( "a" ).prepend( 6 )
		db.root( "a" ).get shouldBe List( 6, 3, 4, 1, 2, 5 )
		
		db.root( "a" ).arrayIterator.drop(3).next.put( "happy" )
		db.root( "a" ).get shouldBe List( 6, 3, 4, "happy", 2, 5 )
		db.root( "a" ).members.toList shouldBe List( 6, 3, 4, "happy", 2, 5 )
	}
}