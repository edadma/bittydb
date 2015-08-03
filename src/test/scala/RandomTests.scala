package ca.hyperreal.bittydb

import collection.mutable.{ListBuffer, HashMap}

import org.scalatest._
import prop.PropertyChecks


class RandomTests extends FreeSpec with PropertyChecks with Matchers {
	
	"mem" in {
		val db = Connection.mem()
		val map = new HashMap[Any, Any]
		
		
	}
	
// 	"disk" in
// 	{
// 	val db = Connection.mem()
// 	
// 	}
}