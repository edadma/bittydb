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
	
	def getting( bytes: Int ) = assert( _buffer.position + bytes <= _size, "attempting to read past end of buffer" )
	
	def putting( bytes: Int ) = sizeHint( _buffer.position + bytes )
	
	def sizeHint( len: Int ) {
		if (len > array.length && len >= 1) {
			val newarray = allocate( len*2 )
			compat.Platform.arraycopy( array, 0, newarray, 0, _size )
			array = newarray
		}
		
		_size = _size max len
	}
}
