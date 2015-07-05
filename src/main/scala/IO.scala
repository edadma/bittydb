package ca.hyperreal.bittydb


abstract class IO
{
	def close
	
	def force
	
	def size: Long
	
	def size_=( l: Long )
	
	def pos: Long
	
	def pos_=( p: Long )
	
	def append: Long
	
	def getByte: Byte
	
	def putByte( b: Byte )
	
	def getUnsignedByte: Int
	
	def getChar: Char
	
	def putChar( c: Char )
	
	def getShort: Short
	
	def putShort( s: Short )
	
	def getUnsignedShort: Int
	
	def getInt: Int
	
	def putInt( i: Int )
	
	def getLong: Long
	
	def putLong( l: Long )
	
	def getDouble: Double
	
	def putDouble( d: Double )
	
	def writeChars( s: String )
	
	if (size == 0) {
		putString( s"BittyDB $VERSION" )
		pos = 0
		force
	}
	
// 	def increase( s: Long ): Long = {
// 		val p = size
// 		
// 		size = size + s
// 		pos = p
// 		p
// 	}
	
	def remaining: Long = size - pos

	def readChars( len: Int ) = {
		val buf = new StringBuilder
		
		for (_ <- 1 to len)
			buf += getChar
			
		buf.toString
	}

	def readOptionChars( len: Int ) = {
		val s = readChars( len )
		
		if (s forall Character.isDefined)
			Some( s )
		else
			None
	}
	
	def getOptionString = {
		if (remaining >= 1) {
			val len = getUnsignedByte
			
			if (remaining >= 2*len)
				readOptionChars( len )
			else
				None
		}
		else
			None
	}
	
	def getString = readChars( getUnsignedByte )
	
	def putString( s: String ) {
		putByte( s.length.asInstanceOf[Byte] )
		writeChars( s )
	}
	
	def todo = sys.error( "not done yet" )
}