package ca.hyperreal.bittydb

import java.nio.ByteBuffer


class ExpandableByteBuffer
{
	private var array: Array[Byte] = null
	private var _buffer: ByteBuffer = null
	private var _size = 0
	
	array = allocate( 16 )
	
	def buffer = _buffer
	
	def size = _size
	
	def size_=( s: Int ) {
		sizeHint( s )
		_size = s
	}
	
	def allocate( capacity: Int ) = {
		val res = new Array[Byte]( capacity )
		val p = 
			if (_buffer eq null)
				0
			else
				_buffer.position
				
		_buffer = ByteBuffer.wrap( res )
		_buffer.position( p )
		res
	}
	
	def need( bytes: Int ) = sizeHint( _buffer.position + bytes )
	
	def sizeHint( len: Int ) {
		if (len > array.length && len >= 1) {
			val newarray = allocate( len*2 )
			compat.Platform.arraycopy( array, 0, newarray, 0, _size )
			array = newarray
		}
		
		_size = _size max len
	}
}

class MemIO extends IO
{
	private lazy val db = new ExpandableByteBuffer
	
	def close {}
	
	def force {}
	
	def size: Long = db.size
	
	def size_=( l: Long ) = db.size = l.asInstanceOf[Int]
	
	def pos: Long = db.buffer.position
	
	def pos_=( p: Long ) = db.buffer.position( p.asInstanceOf[Int] )
	
	def append: Long = {
		pos = size
		size
	}
	
	def getByte: Byte = db.buffer.get
	
	def putByte( b: Int ) {
		db.need( 1 )
		db.buffer.put( b.asInstanceOf[Byte] )
	}
	
	def getUnsignedByte: Int = getByte&0xFF
	
	def getChar: Char = db.buffer.getChar
	
	def putChar( c: Char ) {
		db.need( 2 )
		db.buffer.putChar( c )
	}
	
	def getShort: Short = db.buffer.getShort
	
	def putShort( s: Int ) {
		db.need( 2 )
		db.buffer.putShort( s.asInstanceOf[Short] )
	}
	
	def getUnsignedShort: Int = getShort&0xFFFF
	
	def getInt: Int = db.buffer.getInt
	
	def putInt( i: Int ) {
		db.need( 4 )
		db.buffer.putInt( i )
	}
	
	def getLong: Long = db.buffer.getLong
	
	def putLong( l: Long ) {
		db.need( 8 )
		db.buffer.putLong( l )
	}
	
	def getDouble: Double = db.buffer.getDouble
	
	def putDouble( d: Double ) {
		db.need( 8 )
		db.buffer.putDouble( d )
	}

	def writeByteChars( s: String ) = s foreach {c => putByte( c.asInstanceOf[Int] )}
	
	override def toString = "mem"
}