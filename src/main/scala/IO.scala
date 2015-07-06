package ca.hyperreal.bittydb

import java.nio.charset.Charset


abstract class IO
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
	
	def writeChars( s: String )
	
// 	def increase( s: Long ): Long = {
// 		val p = size
// 		
// 		size = size + s
// 		pos = p
// 		p
// 	}
	
	def getPtr: Long = (getByte.asInstanceOf[Long]&0xFF)<<32 | getInt.asInstanceOf[Long]&0xFFFFFFFFL
	
	def putPtr( l: Long ) {
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

	def readChars( len: Int ) = {
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

// 	def readOptionChars( len: Int ) = {
// 		val s = readChars( len )
// 		
// 		if (s forall Character.isDefined)
// 			Some( s )
// 		else
// 			None
// 	}
// 	
// 	def getOptionString = {
// 		if (remaining >= 1) {
// 			val len = getUnsignedByte
// 			
// 			if (remaining >= 2*len)
// 				readOptionChars( len )
// 			else
// 				None
// 		}
// 		else
// 			None
// 	}
	
	def getString = readChars( getUnsignedByte )
	
	def putString( s: String ) {
		putByte( s.length.asInstanceOf[Byte] )
		writeChars( s )
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