package ca.hyperreal.bittydb

import java.nio.charset.Charset

import collection.mutable.{ListBuffer, HashMap}


abstract class IO extends IOConstants
{
	private [bittydb] var charset: Charset = null
	
	def close
	
	def force
	
	def size: Long
	
	def size_=( l: Long )
	
	def pos: Long
	
	def pos_=( p: Long )
	
	def append: Long
	
	def getByte: Byte
	
	def putByte( b: Int )
	
	def getUnsignedByte: Int
	
	def getChar: Char
	
	def putChar( c: Char )
	
	def getShort: Short
	
	def putShort( s: Int )
	
	def getUnsignedShort: Int
	
	def getInt: Int
	
	def putInt( i: Int )
	
	def getLong: Long
	
	def putLong( l: Long )
	
	def getDouble: Double
	
	def putDouble( d: Double )
	
	def writeByteChars( s: String )
	
//// end of abstract methods
	
// 	def increase( s: Long ): Long = {
// 		val p = size
// 		
// 		size = size + s
// 		pos = p
// 		p
// 	}
	
	def getBig: Long = (getByte.asInstanceOf[Long]&0xFF)<<32 | getInt.asInstanceOf[Long]&0xFFFFFFFFL
	
	def putBig( l: Long ) {
		putByte( (l>>32).asInstanceOf[Int] )
		putInt( l.asInstanceOf[Int] )
	}
	
	def addBig( len: Int ) {
		val cur = pos
		val v = getBig
		
		pos = cur
		putBig( v + len )
	}
	
	def getByte( addr: Long ): Int = {
		pos = addr
		getByte
	}
	
	def putByte( addr: Long, b: Int ) {
		pos = addr
		putByte( b )
	}
	
	def getBig( addr: Long ): Long = {
		pos = addr
		getBig
	}
	
	def putBig( addr: Long, b: Long ) {
		pos = addr
		putBig( b )
	}
	
	def getBytes( len: Int ) = {
		val array = new Array[Byte]( len )
		
		for (i <- 0 until len)
			array(i) = getByte
			
		array
	}
	
	def putBytes( array: Array[Byte] ) {
		for (b <- array)
			putByte( b )
	}
	
	def remaining: Long = size - pos

	def skip( len: Long ) = pos += len
	
	def skipByte = pos += 1
	
	def skipByte( addr: Long ) {
		pos = addr
		skipByte
	}
	
	def skipBig = skip( BWIDTH )
	
	def skipInt = skip( 4 )
	
	def skipLong = skip( 8 )
	
	def skipDouble = skip( 8 )
	
	def skipString = skip( getLen )
	
	def skipValue = skip( 9 )
	
	def pad( n: Long ) =
		for (_ <- 1L to n)
			putByte( 0 )
			
	def padBig = pad( BWIDTH )
	
	def readByteChars( len: Int ) = {
		val buf = new StringBuilder
		
		for (_ <- 1 to len)
			buf += getByte.asInstanceOf[Char]
			
		buf.toString
	}

// 	def readChars( len: Int ) = {
// 		val buf = new StringBuilder
// 		
// 		for (_ <- 1 to len)
// 			buf += getChar
// 			
// 		buf.toString
// 	}

	def getByteString = {
		if (remaining >= 1) {
			val len = getUnsignedByte
			
			if (remaining >= len)
				Some( readByteChars(len) )
			else
				None
		}
		else
			None
	}
	
	def putByteString( s: String ) {
		putByte( s.length.asInstanceOf[Byte] )
		writeByteChars( s )
	}
	
	def getLen: Int = {
		var len = 0
		
		def read {
			val b = getByte
			len = (len << 7) | (b&0x7F)
			
			if ((b&0x80) != 0)
				read
		}

		read
		len
	}
	
	def putLen( l: Int ) {
		if (l == 0)
			putByte( 0 )
		else {
			var downshift = 28
			var compare = 0x10000000
			
			while (l < compare) {
				downshift -= 7
				compare >>= 7
			}
			
			while (downshift >= 0) {
				putByte( (if (downshift > 0) 0x80 else 0) | ((l >> downshift)&0x7F) )
				downshift -= 7
			}
		}
	}
	
	def getString = new String( getBytes(getLen), charset )
	
	def putString( s: String ) = {
		val buf = s.getBytes( charset )
		
		putLen( buf.length )
		putBytes( buf )
	}
	
	def strlen( s: String ) = s.getBytes( charset ).length

	def getType: Int =
		getByte match {
			case POINTER => getByte( getBig )
			case t if t >= 0 => t
			case t => sys.error( "unrecognized value type: " + t )
		}

	def getType( addr: Long ): Int = {
		pos = addr
		getType
	}
	
	def getValue: Any = {
		val cur = pos
		val res =
			getType match {
				case NULL => null
				case FALSE => false
				case TRUE => true
				case INT => getInt
				case LONG => getLong
				case DOUBLE => getDouble
				case STRING => getString
				case EMPTY => Map.empty
				case OBJECT => getObject
				case NIL => Nil
				case ARRAY => getArray
			}
	
		pos = cur + VWIDTH
		res
	}
	
	def putValue( v: Any ) {
		v match {
			case null =>
				putByte( NULL )
				pad( 8 )
			case false =>
				putByte( FALSE )
				pad( 8 )
			case true =>
				putByte( TRUE )
				pad( 8 )
			case a: Int =>
				putByte( INT )
				putInt( a )
				pad( 4 )
			case a: Long =>
				putByte( LONG )
				putLong( a )
			case a: Double =>
				putByte( DOUBLE )
				putDouble( a )
			case a: String =>
				val cur = pos
				putByte( STRING )
				putString( a )
				pad( 9 - (pos - cur) )
			case a: collection.Map[_, _] if a isEmpty =>
				putByte( EMPTY )
				pad( 8 )
			case a: collection.Map[_, _] =>
				putByte( OBJECT )
				sys.error( "OBJECT" )
			case a: collection.Seq[_] if a isEmpty =>
				putByte( NIL )
				pad( 8 )
			case a: collection.Seq[_] if a isEmpty =>
				putByte( ARRAY )
				sys.error( "ARRAY" )
		}
	}
	
	def getValue( addr: Long ): Any = {
		pos = addr
		getValue
	}
	
	def putValue( addr: Long, v: Any ) {
		pos = addr
		putValue( v )
	}
	
	def getObject = {
		val cont = getBig
		val len = getBig
		val start = pos
		var map = Map.empty[Any, Any]

		while (pos - start < len) {
			if (getByte == USED)
				map += getValue -> getValue
			else {
				skipValue
				skipValue
			}
		}

		map
	}
	
	def putObject( a: collection.Map[_, _] ) {
		padBig	// continuation pointer
		
		val start = pos
	
		padBig	// size of object in bytes
		
		for (p <- a)
			putPair( p )
	
		putBig( start, pos - start - BWIDTH )
	}
	
	def putPair( kv: (Any, Any) ) {
		putByte( USED )
		putValue( kv._1 )
		putValue( kv._2 )
	}
	
	def putPair( addr: Long, kv: (Any, Any) ) {
		pos = addr
		putPair( kv )
	}
	
	def getArray = {
		val buf = new ListBuffer[Any]
		val len = getBig
		val start = pos

		while (pos - start < len) {
			if (getByte == USED)
				buf += getValue
		}

		buf.toList
	}
	
	def putArray( s: collection.TraversableOnce[Any] ) {
		padBig	// continuation pointer
		
		val start = pos
	
		padBig	// size of object in bytes
		
		for (e <- s) {
			putByte( USED )
			putValue( s )
		}
	
		putBig( start, pos - start - BWIDTH )
	}
	
	def dump {
		val cur = pos
		val width = 16
		
		pos = 0
		
		def printByte( b: Int ) = print( "%02x ".format(b&0xFF).toUpperCase )
		
		def printChar( c: Int ) = print( if (' ' <= c && c <= '~') c.asInstanceOf[Char] else '.' )
		
		for (line <- 0L until size by width) {
			printf( s"%010x  ", line )
			
			val mark = pos
			
			for (i <- line until ((line + width) min size))
				printByte( getByte )
				
			print( " "*((width - (pos - mark).asInstanceOf[Int])*3 + 2) )
			
			pos = mark
			
			for (i <- line until ((line + width) min size))
				printChar( getByte.asInstanceOf[Int] )
				
			println
		}
		
		pos = cur
	}
}