package xyz.hyperreal.bittydb

import java.io.{ByteArrayInputStream, InputStream}


abstract class Blob {
	def length: Long
	def stream: InputStream
	def bytes( offset: Long, length: Int ): Array[Byte]
}

class ArrayBlob( a: Array[Byte] ) extends Blob {
	val stream = new ByteArrayInputStream( a )
	val length = a.length

	def bytes( offset: Long, length: Int ) = a.slice( offset.toInt, offset.toInt + length )
}

class IOBlob( io: IO, addr: Long, val length: Long ) extends Blob {
	def stream = io.byteInputStream( addr, length )

	def bytes( offset: Long, length: Int ) = {
		val res = new Array[Byte]( length )
		val s = stream

		s.skip( offset )

		s.read( res ) match {
			case `length` =>
			case l => sys.error( s"wrong number of bytes read: $l" )
		}

		s.close
		res
	}
}