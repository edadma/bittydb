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
	
	"arrayIterator" in
	{
	val db = Connection.mem()
	
		db.root.set( "a" -> Nil )
		db.root( "a" ).arrayIterator.isEmpty shouldBe true
		
		db.root( "a" ).append( 5 )
		db.root( "a" ).members.toList shouldBe List( 5 )
		db.root( "a" ).prepend( 1, 2 )
		db.root( "a" ).append( 3 )
		db.root( "a" ).prepend( 4 )
		db.root( "a" ).get shouldBe List( 4, 1, 2, 5, 3 )
		
		db.root( "a" ).arrayIterator.drop(3).next.put( "happy" )
		db.root( "a" ).get shouldBe List( 4, 1, 2, "happy", 3 )
		db.root( "a" ).members.toList shouldBe List( 4, 1, 2, "happy", 3 )
	}
	
	"list (objectIterator)" in
	{
	val db = Connection.mem()
	
		db.root.set( "asdfasdfasdf" -> List(1, 2, 3) )
		db.root.set( "zxcvzxcvzxcv" -> List(4, 5, 6) )
		db.root.set( "qwerqwerqwer" -> List(7, 8, 9) )
		db.root.list map {case (k, v) => (k, v.get)} shouldBe List(
			"asdfasdfasdf" -> List(1, 2, 3), "zxcvzxcvzxcv" -> List(4, 5, 6), "qwerqwerqwer" -> List(7, 8, 9) )
	}
	
	"update (MongoDB style)" in
	{
	val db = Connection.mem()
	
		db.root.set( "test" -> List(Map("a" -> 1, "b" -> "first"), Map("a" -> 2, "b" -> "second"), Map("a" -> 3, "b" -> "third")) )
		db( "test" ).update( Map("a" -> Map("$lt" -> 3)), Map("$set" -> Map("a" -> 123, "b" -> 456)) ) shouldBe 2
		db.root("test").get shouldBe List(Map("a" -> 123, "b" -> 456), Map("a" -> 123, "b" -> 456), Map("a" -> 3, "b" -> "third"))
	}
	
	"insert, find, remove, update (functional style)" in
	{
		val db = Connection.mem( 'charset -> "GB18030", 'uuid -> false )

		db( "test" ).insert( Map("a" -> 1, "b" -> "first"), Map("a" -> 2, "b" -> "second"), Map("a" -> 3, "b" -> "third") )

		db.root("test").get shouldBe List(Map("a" -> 1, "b" -> "first"), Map("a" -> 2, "b" -> "second"), Map("a" -> 3, "b" -> "third"))

		(db( "test" ) find (_("a") < 3) toList) shouldBe List(Map("a" -> 1, "b" -> "first"), Map("a" -> 2, "b" -> "second"))

		db( "test" ) remove (_("a") in Set(1, 2))
		db.root("test").get shouldBe List(Map("a" -> 3, "b" -> "third"))

		db( "test" ) update (_("a") === 3, "b" -> "第三")
		db.root("test").get shouldBe List(Map("a" -> 3, "b" -> "第三"))
	}
}