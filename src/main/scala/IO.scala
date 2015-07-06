package ca.hyperreal.bittydb

import java.nio.charset.Charset

import collection.mutable.HashMap


object IO {
	val NULL = 0
	val TRUE = 1
	val FALSE = 2
	val INT = 3
	val LONG = 4
	val BIGINT = 5
	val DOUBLE = 6
	val DECIMAL = 7
	val NIL = 8
	val STRING = 9
	val EMPTY = 10
	val OBJECT = 11
	val POINTER = 12
	
	val SWIDTH = 5	// size width
	val PWIDTH = 5	// pointer width
}

abstract class IO
{
	import IO._
	
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

	def skip( len: Long ) = size += len
	
	def skipSize = skip( SWIDTH )
	
	def skipPointer = skip( PWIDTH )
	
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
	
	def getValue: Any =
		(getByte match {
			case POINTER =>
				pos = getBig
				getByte
			case b =>
				b
		}) match {
			case NULL => null
			case FALSE => false
			case TRUE => true
			case INT => getInt
			case LONG => getLong
			case EMPTY => Map.empty
			case OBJECT =>
				val cont = getBig
				val len = getBig
				val start = pos
				var map = Map.empty[Any, Any]
				
				while (pos - start < len)
					map += getValue -> getValue
					
				map
		}
	
	def putValue( v: Any ) {
		v match {
			case null =>
				putByte( NULL )
			case false =>
				putByte( FALSE )
			case true =>
				putByte( TRUE )
			case a: Int =>
				putByte( INT )
				putInt( a )
			case a: Long =>
				putByte( LONG )
				putLong( a )
			case a: Double =>
				putByte( DOUBLE )
				putDouble( a )
			case "" =>
				putByte( NIL )
			case a: String =>
				putByte( STRING )
				putString( a )
			case a: collection.Map[_, _] if a isEmpty =>
				putByte( EMPTY )
			case a: collection.Map[_, _] =>
				putByte( OBJECT )
				putBig( 0 )	// continuation pointer
				
			val start = pos
			
				skipSize	// size of object in bytes
			
				for ((k, v) <- a) {
					putValue( k )
					putValue( v )
				}
				
			val len = pos - start - SWIDTH
			
				pos = start
				putBig( len )
		}
	}
	
	def dump {
		val cur = pos
		val width = 16
		
		pos = 0
		
		def printByte( b: Int ) = print( "%02x ".format(b).toUpperCase )
		
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