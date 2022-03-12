package xyz.hyperreal.bittydb

import java.nio.ByteBuffer


class ExpandableByteBuffer( maxsize: Int )
{
	private var array: Array[Byte] = _
	private var _buffer: ByteBuffer = _
	private var _size = 0

	array = allocate( 1024 )
	
	def buffer = _buffer
	
	def size = _size
	
	def size_=( s: Int ) {
		val cur = _buffer.position()
		
		sizeHint( s )
		_size = s
		
		if (cur > _size)
			_buffer.position( _size )
	}
	
	def allocate( capacity: Int ) = {
		val res = new Array[Byte]( capacity )
		val p = 
			if (_buffer eq null)
				0
			else
				_buffer.position()
				
		_buffer = ByteBuffer.wrap( res )
		_buffer.position( p )
		res
	}
	
	def getting( bytes: Int ) = assert( _buffer.position() + bytes <= _size, "attempting to read past end of buffer" )
	
	def putting( bytes: Int ) = {
		if (_buffer.position() + bytes.toLong > maxsize)
			sys.error( "size overflow" )

		sizeHint( _buffer.position() + bytes )
	}

	def sizeHint( hint: Int ) {
		if (hint > array.length && hint >= 1) {
			val newarray = allocate( bitCeiling(hint).toInt )

			compat.Platform.arraycopy( array, 0, newarray, 0, _size )
			array = newarray
		}

		if (hint > _size)
			_size = hint
	}
}